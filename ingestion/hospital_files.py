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
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

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
    etag: str | None = None
    last_modified: str | None = None


@dataclass
class SkipDecision:
    skip: bool
    reason: str


def _as_utc(value):
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value


def _fetch_headers(spec: HospitalFileSpec, client: httpx.Client):
    """Response headers only, never the body. Try HEAD first; some servers reject
    it, so fall back to a GET that is closed before the body is read."""
    timeout = httpx.Timeout(30.0)
    try:
        resp = client.head(spec.url, follow_redirects=True, timeout=timeout)
        if resp.status_code == 200 and (resp.headers.get("etag") or resp.headers.get("last-modified")):
            return resp.headers
        with client.stream("GET", spec.url, follow_redirects=True, timeout=timeout) as r:
            return r.headers if r.status_code == 200 else None
    except httpx.HTTPError:
        return None


def decide_skip(spec: HospitalFileSpec, client: httpx.Client, prev: dict | None,
                force: bool = False) -> SkipDecision:
    """Decide whether the file on the server is unchanged since our last download.

    Conservative by design: we only skip when the server gives a reliable signal.
    In order: (1) ETag, (2) Last-Modified, (3) "not modified since our last
    download" with a matching size. With no usable signal we download.
    """
    if force:
        return SkipDecision(False, "force_download is on")
    if not prev:
        return SkipDecision(False, "no earlier successful download")

    path = prev.get("landing_path")
    if not path or not os.path.exists(path) or os.path.getsize(path) != prev.get("file_bytes"):
        return SkipDecision(False, "earlier file is missing or its size changed on disk")

    downloaded_at = _as_utc(prev.get("event_ts"))
    max_age = config.FILES.max_age_days
    if downloaded_at and datetime.now(timezone.utc) - downloaded_at > timedelta(days=max_age):
        return SkipDecision(False, f"earlier download is over {max_age} days old; refreshing")

    headers = _fetch_headers(spec, client)
    if headers is None:
        return SkipDecision(False, "could not read headers from the server")
    etag, last_modified = headers.get("etag"), headers.get("last-modified")

    if etag and prev.get("etag"):
        same = etag == prev["etag"]
        return SkipDecision(same, "ETag unchanged" if same else "ETag changed")
    if last_modified and prev.get("last_modified"):
        same = last_modified == prev["last_modified"]
        return SkipDecision(same, "Last-Modified unchanged" if same else "Last-Modified changed")
    if last_modified and downloaded_at:
        try:
            modified = _as_utc(parsedate_to_datetime(last_modified))
        except (TypeError, ValueError):
            modified = None
        if modified is not None:
            length = headers.get("content-length")
            length_ok = length is None or (length.isdigit() and int(length) == prev.get("file_bytes"))
            if modified <= downloaded_at and length_ok:
                return SkipDecision(True, "not modified since the last download")
            return SkipDecision(False, "modified after the last download, or the size differs")
    return SkipDecision(False, "server gave no reliable change signal")


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
                etag = resp.headers.get("etag")
                last_modified = resp.headers.get("last-modified")
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
                spec.key, "succeeded", dest, total, digest.hexdigest(), content_type, attempt,
                etag=etag, last_modified=last_modified,
            )

        except httpx.TransportError as exc:      # timeouts, connection resets, DNS
            last_error = f"{type(exc).__name__}: {exc}"
            _remove_quietly(dest)
            _backoff(attempt, cfg.retry)
        except (ValueError, OSError) as exc:     # bad content or write problem: don't retry
            _remove_quietly(dest)
            return failed(str(exc), attempt)
        except BaseException:                    # e.g. notebook cell interrupted (KeyboardInterrupt)
            _remove_quietly(dest)                # never leave a partial file that looks like raw data
            raise

    return failed(f"gave up after {cfg.retry.max_attempts} attempts: {last_error}", cfg.retry.max_attempts)


def download_all(
    specs: list[HospitalFileSpec], client: httpx.Client, run_id: str
) -> list[DownloadResult]:
    """Download every file; one failure never stops the others."""
    return [download_hospital_file(spec, client, run_id) for spec in specs]
