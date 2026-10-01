"""
Ingestion entry point. Run this as a Databricks notebook cell or as a Job task
(`python -m ingestion.run_ingest`).

What it does, per dataset in config.DATASETS:
  1. Page through the source API (backoff/retry handled in http_client.py).
  2. Write each page as a raw JSON file into a Unity Catalog Volume, under
     /Volumes/<catalog>/<schema>/<volume>/<dataset_key>/run_date=YYYY-MM-DD/page_N.json
  3. Log every page to the manifest Delta table — success or failure.
  4. Never let one bad page kill the whole run: a page-level exception is
     caught, logged as "failed", and the loop moves to the next page/dataset.
  5. Download each hospital price file in config.HOSPITAL_FILES (streamed, with
     checksum) into <volume>/hospital_price_files/ and log it the same way.

No S3, no GCS — the landing path IS the Databricks Volume, so this runs
identically in a notebook or a Job cluster without extra cloud wiring.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date

import httpx

from . import sanity
from .cms_data_api import fetch_dataset_pages as fetch_cms_data_pages
from .config import DATASETS, FILES, HOSPITAL_FILES, VOLUME_ROOT, DatasetSpec, HospitalFileSpec
from .hospital_files import download_hospital_file
from .http_client import RetryableRequestError
from .manifest import ensure_manifest_table, log_event, new_run_id, run_summary
from .provider_data_catalog import fetch_dataset_pages as fetch_provider_pages

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("cms_ingestion.run")


def _landing_dir(dataset_key: str) -> str:
    run_date = date.today().isoformat()
    return f"{VOLUME_ROOT}/{dataset_key}/run_date={run_date}"


def _write_page(landing_dir: str, page_number: int, rows: list[dict]) -> str:
    """Write one page of rows as a JSON file directly to the UC Volume path.
    Volumes are just POSIX paths from the driver's perspective, so this is
    a plain file write — no separate SDK/client needed."""
    import os

    os.makedirs(landing_dir, exist_ok=True)
    file_path = f"{landing_dir}/page_{page_number:05d}.json"
    with open(file_path, "w") as f:
        json.dump(rows, f)
    return file_path


def ingest_dataset(client: httpx.Client, spark, run_id: str, spec: DatasetSpec) -> None:
    landing_dir = _landing_dir(spec.key)
    log_event(spark, run_id, spec.key, spec.source, status="started", landing_path=landing_dir)

    if spec.source == "cms_data_api":
        page_iter = fetch_cms_data_pages(client, spec.identifier, filters=spec.filters)
    elif spec.source == "provider_data_catalog":
        page_iter = fetch_provider_pages(client, spec.identifier)
    else:
        raise ValueError(f"Unknown source '{spec.source}' for dataset '{spec.key}'")

    total_rows = 0
    try:
        for page_number, rows in page_iter:
            try:
                file_path = _write_page(landing_dir, page_number, rows)
                log_event(
                    spark, run_id, spec.key, spec.source,
                    status="succeeded", page_number=page_number,
                    row_count=len(rows), landing_path=file_path,
                )
                total_rows += len(rows)
            except Exception as page_exc:
                # A single bad page doesn't kill the dataset — log and continue.
                logger.exception("Failed writing page %s for %s", page_number, spec.key)
                log_event(
                    spark, run_id, spec.key, spec.source,
                    status="failed", page_number=page_number,
                    error_message=str(page_exc),
                )
    except RetryableRequestError as exc:
        # Retries were exhausted for a page fetch itself (not a write failure).
        logger.error("Exhausted retries fetching %s: %s", spec.key, exc)
        log_event(
            spark, run_id, spec.key, spec.source,
            status="failed", error_message=str(exc),
        )
        return
    except Exception as exc:
        # Anything else raised while fetching: a non-retryable HTTP error (e.g.
        # 404 for a bad dataset ID, which http_client re-raises immediately),
        # invalid JSON, or a protocol error. Log it and let the run continue.
        logger.exception("Unexpected error fetching %s", spec.key)
        log_event(
            spark, run_id, spec.key, spec.source,
            status="failed", error_message=f"{type(exc).__name__}: {exc}",
        )
        return

    logger.info("Finished dataset=%s total_rows=%s landing_dir=%s", spec.key, total_rows, landing_dir)


HOSPITAL_FILE_SOURCE = "hospital_price_file"


def ingest_hospital_file(client: httpx.Client, spark, run_id: str, spec: HospitalFileSpec) -> None:
    """Download one hospital price file and log the outcome to the manifest.
    download_hospital_file never raises, so one bad hospital can't stop the run."""
    log_event(spark, run_id, spec.key, HOSPITAL_FILE_SOURCE, status="started")
    result = download_hospital_file(spec, client, run_id)
    log_event(
        spark, run_id, spec.key, HOSPITAL_FILE_SOURCE,
        status=result.status,
        error_message=result.error,
        landing_path=result.path,
        file_bytes=result.bytes_written if result.status == "succeeded" else None,
        sha256=result.sha256,
    )
    logger.info("Hospital file key=%s status=%s bytes=%s", spec.key, result.status, result.bytes_written)


def main(spark, datasets: list[DatasetSpec] | None = None,
         hospital_files: list[HospitalFileSpec] | None = None,
        run_checks: bool = True, verify_checksum: bool = True) -> str:
    """
    `spark` is the active SparkSession — in a Databricks notebook this is
    just the `spark` global already in scope; pass it explicitly here so
    this module has no hidden dependency on notebook context.

    `datasets` / `hospital_files` default to the lists in config. Pass a
    shorter list to test one source without editing config.py, e.g.
    `main(spark, datasets=[], hospital_files=config.HOSPITAL_FILES[:1])`.

    After ingesting, a sanity report is printed for every source (see
    sanity.py). If any check FAILS, a SanityCheckError is raised so a Job
    task shows as failed; the run_id is in the message. Pass
    `verify_checksum=False` to skip re-reading large files, or
    `run_checks=False` to skip the checks entirely.
    """
    datasets = DATASETS if datasets is None else datasets
    hospital_files = HOSPITAL_FILES if hospital_files is None else hospital_files
    if not datasets and not hospital_files:
        raise ValueError(
            "Nothing to ingest — add a DatasetSpec to config.DATASETS "
            "and/or a HospitalFileSpec to config.HOSPITAL_FILES."
        )

    ensure_manifest_table(spark)
    run_id = new_run_id()
    logger.info(
        "Starting ingestion run_id=%s for %s dataset(s) and %s hospital file(s)",
        run_id, len(datasets), len(hospital_files),
    )

    timings: dict[str, float] = {}

    if datasets:
        with httpx.Client(headers={"User-Agent": "nss-de-final-project/1.0"}) as client:
            for spec in datasets:
                started = time.monotonic()
                ingest_dataset(client, spark, run_id, spec)
                timings[spec.key] = time.monotonic() - started

    if hospital_files:
        # Separate client: some hospital sites reject non-browser User-Agents.
        with httpx.Client(headers={"User-Agent": FILES.user_agent}) as file_client:
            for spec in hospital_files:
                started = time.monotonic()
                ingest_hospital_file(file_client, spark, run_id, spec)
                timings[spec.key] = time.monotonic() - started

    logger.info("Run %s complete. Summary:", run_id)
    run_summary(spark, run_id).show(truncate=False)

    print("\nTime per source")
    for key, seconds in timings.items():
        print(f"  {key:32} {seconds:8.1f} s")
    print(f"  {'total':32} {sum(timings.values()):8.1f} s")

    if run_checks:
        failures = _run_sanity_checks(spark, run_id, [*datasets, *hospital_files], verify_checksum)
        if failures:
            raise sanity.SanityCheckError(f"run_id={run_id}: " + "; ".join(failures)) 
    return run_id


def _run_sanity_checks(spark, run_id: str, specs: list, verify_checksum: bool) -> list[str]:
    """Print a report for every source; return a description of each source that failed."""
    failures: list[str] = []
    for spec in specs:
        results = sanity.check_source(spark, run_id, spec, verify_checksum)
        sanity.print_report(spec.key, results)
        failed = [r.name for r in results if r.status == sanity.FAIL]
        if failed:
            failures.append(f"{spec.key}: {', '.join(failed)}")
    return failures

    
if __name__ == "__main__":
    # For Job-cluster execution: `spark` is provided by the Databricks
    # runtime as a global even in a plain .py entry point run as a Job task.
    main(spark)  # noqa: F821
