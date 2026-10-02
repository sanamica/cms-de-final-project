"""
Sanity checks for ONE ingested source, run right after that source lands.

Each check returns pass / warn / fail:
  fail  = the data is not trustworthy (the Job task should fail)
  warn  = worth a human look, but not proof of a problem

The checks compare three things that should agree: what the manifest table
says happened, what is actually on disk in the Volume, and what the config
expects (minimum rows or size, required columns, expected format).

Only the manifest lookup needs Spark; everything else is plain Python.
"""

from __future__ import annotations

import csv
import hashlib
import io
import itertools
import json
import os
import re
from dataclasses import dataclass

from . import config
from .config import DatasetSpec, HospitalFileSpec

PASS, WARN, FAIL = "pass", "warn", "fail"

_GENERIC_WORDS = {"hospital", "medical", "center", "health", "healthcare", "system", "regional"}


@dataclass
class CheckResult:
    name: str
    status: str
    detail: str = ""


class SanityCheckError(Exception):
    """Raised when at least one check fails."""


def _manifest_rows(spark, run_id: str, key: str) -> list[dict]:
    query = (
        "SELECT status, page_number, row_count, error_message, landing_path, file_bytes, sha256, note "
        f"FROM {config.MANIFEST_TABLE} WHERE run_id = '{run_id}' AND dataset_key = '{key}'"
    )
    return [r.asDict() if hasattr(r, "asDict") else dict(r) for r in spark.sql(query).collect()]


def _event_checks(rows: list[dict]) -> list[CheckResult]:
    started = [r for r in rows if r["status"] == "started"]
    failed = [r for r in rows if r["status"] == "failed"]
    results = [CheckResult("started_logged", PASS if started else FAIL,
                           "run start is in the manifest" if started else "no 'started' row in the manifest")]
    if failed:
        errors = sorted({str(r["error_message"])[:120] for r in failed})
        results.append(CheckResult("no_failed_events", FAIL, f"{len(failed)} failed event(s): {'; '.join(errors)}"))
    else:
        results.append(CheckResult("no_failed_events", PASS, "no failed events"))
    return results


# ---------------------------------------------------------------------------
# CMS datasets (JSON pages)
# ---------------------------------------------------------------------------
def check_dataset(spark, run_id: str, spec: DatasetSpec) -> list[CheckResult]:
    rows = _manifest_rows(spark, run_id, spec.key)
    results = _event_checks(rows)

    pages = [r for r in rows if r["status"] == "succeeded" and r["page_number"] is not None]
    results.append(CheckResult("pages_landed", PASS if pages else FAIL, f"{len(pages)} page(s) logged as succeeded"))
    if not pages:
        return results

    nums = sorted(r["page_number"] for r in pages)
    contiguous = nums == list(range(nums[0], nums[-1] + 1))
    results.append(CheckResult("pages_contiguous", PASS if contiguous else FAIL,
                               f"pages {nums[0]}..{nums[-1]}" if contiguous else f"gaps in page numbers: {nums}"))

    missing = [r["landing_path"] for r in pages if not r["landing_path"] or not os.path.exists(r["landing_path"])]
    results.append(CheckResult("files_exist", FAIL if missing else PASS,
                               f"{len(missing)} logged file(s) missing from the Volume" if missing
                               else "every logged page file exists"))

    landing_dir = os.path.dirname(pages[0]["landing_path"] or "")
    if landing_dir and os.path.isdir(landing_dir):
        expected = {os.path.basename(r["landing_path"]) for r in pages if r["landing_path"]}
        extra = sorted(set(os.listdir(landing_dir)) - expected)
        results.append(CheckResult("no_stale_files", WARN if extra else PASS,
                                   f"{len(extra)} file(s) in the folder are not from this run (same-day leftovers?)"
                                   if extra else "folder holds only this run's pages"))
    if missing:
        return results

    total, first_keys, problem = 0, None, None
    for r in pages:
        try:
            with open(r["landing_path"]) as fh:
                data = json.load(fh)
        except ValueError as exc:
            problem = f"{os.path.basename(r['landing_path'])} is not valid JSON: {exc}"
            break
        if not isinstance(data, list) or (data and not isinstance(data[0], dict)):
            problem = f"{os.path.basename(r['landing_path'])} is not a JSON array of objects"
            break
        total += len(data)
        if first_keys is None and data:
            first_keys = {k.lower() for k in data[0]}
    if problem:
        results.append(CheckResult("pages_parse", FAIL, problem))
        return results
    results.append(CheckResult("pages_parse", PASS, "every page is a JSON array of objects"))

    manifest_total = sum(r["row_count"] or 0 for r in pages)
    results.append(CheckResult("rows_reconcile", PASS if total == manifest_total else FAIL,
                               f"{total} rows on disk, {manifest_total} in manifest"))
    results.append(CheckResult("min_rows", PASS if total >= spec.min_rows else FAIL,
                               f"{total} rows (minimum {spec.min_rows})"))
    if spec.required_columns:
        absent = [c for c in spec.required_columns if c.lower() not in (first_keys or set())]
        results.append(CheckResult("required_columns", FAIL if absent else PASS,
                                   f"missing: {absent}" if absent else "all required columns present"))
    return results


# ---------------------------------------------------------------------------
# Hospital price files (CSV / JSON / ZIP)
# ---------------------------------------------------------------------------
def _read_head(path: str, n: int = 65536) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(n)


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_problem(fmt: str, head: bytes) -> str | None:
    text = head.decode("utf-8-sig", errors="ignore").lower()
    if fmt == "zip":
        return None if head.startswith(b"PK") else "missing zip file signature"
    if fmt == "csv":
        return None if "standard_charge" in text else "no 'standard_charge' column in the first 64 KB"
    if fmt == "json":
        ok = "standard_charge_information" in text or "hospital_name" in text
        return None if ok else "no price transparency keys in the first 64 KB"
    return None


def _hospital_name_in_file(fmt: str, head: bytes) -> str | None:
    text = head.decode("utf-8-sig", errors="ignore")
    if fmt == "json":
        match = re.search(r'"hospital_name"\s*:\s*"([^"]+)"', text)
        return match.group(1) if match else None
    if fmt == "csv":
        rows = list(itertools.islice(csv.reader(io.StringIO(text)), 2))
        if len(rows) == 2:
            header = [c.strip().lower() for c in rows[0]]
            if "hospital_name" in header:
                i = header.index("hospital_name")
                return rows[1][i] if i < len(rows[1]) else None
    return None


def _tokens(name: str) -> set[str]:
    return set(re.findall(r"[a-z]{4,}", name.lower())) - _GENERIC_WORDS


def check_hospital_file(spark, run_id: str, spec: HospitalFileSpec, verify_checksum: bool = True) -> list[CheckResult]:
    rows = _manifest_rows(spark, run_id, spec.key)
    results = _event_checks(rows)

    done = [r for r in rows if r["status"] in ("succeeded", "skipped")]
    if len(done) != 1:
        results.append(CheckResult("one_succeeded_row", FAIL,
                                   f"expected 1 succeeded or skipped row, found {len(done)}"))
        return results
    row = done[0]
    if row["status"] == "skipped":
        results.append(CheckResult("download_or_skip", PASS,
                                   f"skipped ({row.get('note')}); existing file re-verified below"))
    path = row["landing_path"]
    if not path or not os.path.exists(path):
        results.append(CheckResult("file_exists", FAIL, f"{path} is not in the Volume"))
        return results
    results.append(CheckResult("file_exists", PASS, path))

    size = os.path.getsize(path)
    results.append(CheckResult("size_matches_manifest", PASS if size == row["file_bytes"] else FAIL,
                               f"{size} bytes on disk, {row['file_bytes']} in manifest"))
    results.append(CheckResult("min_bytes", PASS if size >= spec.min_bytes else FAIL,
                               f"{size} bytes (minimum {spec.min_bytes})"))
    if verify_checksum:
        same = _sha256(path) == row["sha256"]
        results.append(CheckResult("checksum_matches", PASS if same else FAIL,
                                   "SHA-256 matches the manifest" if same else "SHA-256 differs from the manifest"))

    head = _read_head(path)
    problem = _format_problem(spec.file_format, head)
    results.append(CheckResult("format_looks_right", WARN if problem else PASS,
                               problem or f"looks like a {spec.file_format} price file"))

    found = _hospital_name_in_file(spec.file_format, head)
    if found is None:
        results.append(CheckResult("hospital_name_in_file", WARN, "could not read hospital_name from the file header"))
    elif _tokens(spec.hospital_name) & _tokens(found):
        results.append(CheckResult("hospital_name_in_file", PASS, f"file says '{found}'"))
    else:
        results.append(CheckResult("hospital_name_in_file", WARN,
                                   f"file says '{found}', which does not obviously match '{spec.hospital_name}'"))
    return results


# ---------------------------------------------------------------------------
def check_source(spark, run_id: str, spec, verify_checksum: bool = True) -> list[CheckResult]:
    if isinstance(spec, DatasetSpec):
        return check_dataset(spark, run_id, spec)
    if isinstance(spec, HospitalFileSpec):
        return check_hospital_file(spark, run_id, spec, verify_checksum)
    raise TypeError(f"Unknown source spec type: {type(spec).__name__}")


def print_report(key: str, results: list[CheckResult]) -> None:
    print(f"\nSanity checks for {key}")
    for r in results:
        print(f"  [{r.status.upper():4}] {r.name}: {r.detail}")


def assert_ok(key: str, results: list[CheckResult]) -> None:
    failed = [r for r in results if r.status == FAIL]
    if failed:
        raise SanityCheckError(f"{key}: " + "; ".join(f"{r.name} ({r.detail})" for r in failed))
