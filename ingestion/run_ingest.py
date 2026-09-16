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

No S3, no GCS — the landing path IS the Databricks Volume, so this runs
identically in a notebook or a Job cluster without extra cloud wiring.
"""

from __future__ import annotations

import json
import logging
from datetime import date

import httpx

from .cms_data_api import fetch_dataset_pages as fetch_cms_data_pages
from .config import DATASETS, VOLUME_ROOT, DatasetSpec
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

    logger.info("Finished dataset=%s total_rows=%s landing_dir=%s", spec.key, total_rows, landing_dir)


def main(spark) -> str:
    """
    `spark` is the active SparkSession — in a Databricks notebook this is
    just the `spark` global already in scope; pass it explicitly here so
    this module has no hidden dependency on notebook context.
    """
    if not DATASETS:
        raise ValueError(
            "config.DATASETS is empty — add at least one DatasetSpec "
            "(dataset UUID/ID) before running ingestion."
        )

    ensure_manifest_table(spark)
    run_id = new_run_id()
    logger.info("Starting ingestion run_id=%s for %s dataset(s)", run_id, len(DATASETS))

    with httpx.Client(headers={"User-Agent": "nss-de-final-project/1.0"}) as client:
        for spec in DATASETS:
            ingest_dataset(client, spark, run_id, spec)

    logger.info("Run %s complete. Summary:", run_id)
    run_summary(spark, run_id).show(truncate=False)
    return run_id


if __name__ == "__main__":
    # For Job-cluster execution: `spark` is provided by the Databricks
    # runtime as a global even in a plain .py entry point run as a Job task.
    main(spark)  # noqa: F821
