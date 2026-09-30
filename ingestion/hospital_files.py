"""
Streaming download of hospital price transparency files into the UC Volume.

Design notes
------------
* Files are streamed to the Volume in chunks, never held in memory. Price files
  can be hundreds of MB.
* Nothing here raises for a bad file. Each call returns a DownloadResult so one
  broken hospital never stops the rest of the run. The caller (run_ingest.py)
  writes the started/succeeded/failed rows to the manifest table.
* A 200 response is not proof of a good file. Hospital sites often return an
  HTML error or login page with status 200, so the first bytes are sniffed.
* Retries (with exponential backoff) apply only to transport errors and
  retryable HTTP statuses. Content problems fail immediately.
* The file is written straight to its final path; on failure the partial file
  is removed so a bad download never looks like raw data.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from . import config
from .config import HospitalFileSpec

log = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    key: str
    status: str                  # "succeeded" | "failed"
    path: str | None
    bytes_written: int
    sha256: str | None
    content_type: str | None
    attempts: int
    error: str | None = None


def _sniff_problem(spec: HospitalFileSpec, head: bytes) -> str | None:
    """Return a reason if the payload is not what we expected, else None."""
    if not head:
        return "empty response body"
    start = head.lstrip(b"\xef\xbb\xbf").lstrip()[:32].lower()
    if spec.file_format in ("csv", "json") and start.startswith((b"<!doctype", b"<html")):
        return "received an HTML page instead of a data file"
    if spec.file_format == "json" and start[:1] not in (b"{", b"["):
        return "expected JSON but payload does not start with { or ["
    if spec.file_format == "zip" and not head.startswith(b"PK"):
        return "expected a zip archive but the file signature is missing"
    return None


def _backoff(attempt: int, retry: config.RetryConfig) -> None:
    if attempt < retry.max_attempts:
        time.sleep(min(retry.backoff_base_seconds * 2 ** (attempt - 1), retry.backoff_max_seconds))


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def download_hospital_file(
    spec: HospitalFileSpec, client: httpx.Client, run_id: str
) -> DownloadResult:
    cfg = config.FILES
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest_dir = f"{config.PRICE_FILES_ROOT}/{spec.key}/ingest_date={ingest_date}/run_id={run_id}"
    dest = f"{dest_dir}/{spec.key}.{spec.file_format}"
    timeout = httpx.Timeout(
        connect=cfg.connect_timeout_seconds, read=cfg.read_timeout_seconds, write=30.0, pool=30.0
    )

    def failed(error: str, attempts: int, content_type: str | None = None) -> DownloadResult:
        log.error("download failed key=%s attempts=%s error=%s", spec.key, attempts, error)
        return DownloadResult(spec.key, "failed", None, 0, None, content_type, attempts, error)

    try:
        os.makedirs(dest_dir, exist_ok=True)
    except OSError as exc:
        return failed(f"could not create landing directory: {exc}", 0)

    last_error = "no attempt made"
    for attempt in range(1, cfg.retry.max_attempts + 1):
        try:
            with client.stream("GET", spec.url, follow_redirects=True, timeout=timeout) as resp:
                content_type = resp.headers.get("content-type")
                if resp.status_code in cfg.retry.retryable_statuses:
                    last_error = f"HTTP {resp.status_code}"
                    _backoff(attempt, cfg.retry)
                    continue
                if resp.status_code != 200:
                    return failed(f"HTTP {resp.status_code}", attempt, content_type)

                digest, total, head = hashlib.sha256(), 0, b""
                with open(dest, "wb") as fh:
                    for chunk in resp.iter_bytes(cfg.chunk_bytes):
                        if not head:
                            head = chunk[:512]
                        total += len(chunk)
                        if total > cfg.max_bytes:
                            raise ValueError(f"file exceeds size cap of {cfg.max_bytes} bytes")
                        digest.update(chunk)
                        fh.write(chunk)

            problem = _sniff_problem(spec, head)
            if problem:
                raise ValueError(problem)

            log.info("download ok key=%s bytes=%s attempts=%s", spec.key, total, attempt)
            return DownloadResult(
                spec.key, "succeeded", dest, total, digest.hexdigest(), content_type, attempt
            )

        except httpx.TransportError as exc:      # timeouts, connection resets, DNS
            last_error = f"{type(exc).__name__}: {exc}"
            _remove_quietly(dest)
            _backoff(attempt, cfg.retry)
        except (ValueError, OSError) as exc:     # bad content or write problem: don't retry
            _remove_quietly(dest)
            return failed(str(exc), attempt)

    return failed(f"gave up after {cfg.retry.max_attempts} attempts: {last_error}", cfg.retry.max_attempts)


def download_all(
    specs: list[HospitalFileSpec], client: httpx.Client, run_id: str
) -> list[DownloadResult]:
    """Download every file; one failure never stops the others."""
    return [download_hospital_file(spec, client, run_id) for spec in specs]
