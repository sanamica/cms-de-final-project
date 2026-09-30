"""
Configuration for the CMS ingestion module.

Everything here is intentionally overridable via environment variables (or
Databricks widgets, if you call `apply_widget_overrides` from a notebook) so
the same code runs in dev/test/prod without edits.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Unity Catalog landing locations
# ---------------------------------------------------------------------------
# Land raw API responses as JSON files in a UC Volume. No S3/GCS bucket —
# the Volume IS the storage layer. Bronze Delta tables are created on top of
# this path with Auto Loader / COPY INTO in the Week 1 notebook.
CATALOG = os.environ.get("CMS_CATALOG", "main")
BRONZE_SCHEMA = os.environ.get("CMS_BRONZE_SCHEMA", "bronze")
LANDING_VOLUME = os.environ.get("CMS_LANDING_VOLUME", "raw_landing")

VOLUME_ROOT = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{LANDING_VOLUME}"

# Manifest table (run/page level ingestion log) — lives as a managed Delta
# table, not a file, so you can query it directly in SQL/Metabase.
MANIFEST_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.ingestion_manifest"


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
# 1) data.cms.gov main API — dataset UUID + /data endpoint, size/offset paging.
CMS_DATA_API_BASE = "https://data.cms.gov/data-api/v1"

# 2) Provider Data Catalog — separate API, separate paging/filter syntax.
CMS_PROVIDER_DATA_API_BASE = "https://data.cms.gov/provider-data/api/1"


# ---------------------------------------------------------------------------
# HTTP / retry behavior
# ---------------------------------------------------------------------------
@dataclass
class RetryConfig:
    max_attempts: int = 5
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 30.0
    # Status codes worth retrying. 429 = rate limited, 5xx = server-side.
    retryable_statuses: tuple = (408, 429, 500, 502, 503, 504)


@dataclass
class HttpConfig:
    timeout_seconds: float = 30.0
    page_size: int = 5000  # data.cms.gov max page size
    provider_page_size: int = 500  # Provider Data Catalog practical page size
    retry: RetryConfig = field(default_factory=RetryConfig)


HTTP = HttpConfig()


# ---------------------------------------------------------------------------
# Dataset registry
# ---------------------------------------------------------------------------
# Fill in the dataset UUIDs / identifiers you decide to pull. Keeping this as
# a plain list (rather than scattering IDs through the code) means Week 3's
# "what does this pipeline actually ingest" question has one obvious answer.
@dataclass
class DatasetSpec:
    key: str                 # short name used in landing path + manifest
    source: str               # "cms_data_api" or "provider_data_catalog"
    identifier: str           # dataset UUID
    filters: dict | None = None  # optional column filters, source-specific


DATASETS: list[DatasetSpec] = [
    # --- data.cms.gov main API examples (replace UUIDs with your picks) ---
    # DatasetSpec(
    #     key="inpatient_hospitals_by_provider",
    #     source="cms_data_api",
    #     identifier="REPLACE_WITH_DATASET_UUID",
    # ),

    # --- Provider Data Catalog examples ---
    DatasetSpec(
        key="hospital_general_information",
        source="provider_data_catalog",
        identifier="xubh-q36u",
    ),
    # --- Outcomes for the THA/TKA readmission question ---
    # VERIFY these identifiers with the list_datasets cell in your setup
    # notebook before the first run. Each file covers many measures; keep them
    # whole in bronze and filter to the hip/knee measure IDs in silver.
    DatasetSpec(
        key="unplanned_hospital_visits",       # includes hip/knee 30-day readmission
        source="provider_data_catalog",
        identifier="632h-zaca",
    ),
    DatasetSpec(
        key="complications_and_deaths",        # includes hip/knee complication rate
        source="provider_data_catalog",
        identifier="ynj2-r877",
    ),
    DatasetSpec(
        key="hrrp_readmissions_reduction",     # excess readmission ratios (THA/TKA)
        source="provider_data_catalog",
        identifier="9n3s-kdb3",
    ),
]


# ---------------------------------------------------------------------------
# Hospital price transparency files (negotiated-rate machine-readable files)
# ---------------------------------------------------------------------------
# Unlike the CMS datasets above, these are plain file downloads from each
# hospital's own website, so they get their own spec + downloader
# (see hospital_files.py). One entry per hospital file.
@dataclass
class HospitalFileSpec:
    key: str              # short name used in landing path + manifest, e.g. "vumc_mrf"
    hospital_name: str
    ccn: str              # CMS Certification Number (6 chars, keep as string!)
    url: str              # direct download URL of the machine-readable file
    file_format: str      # "csv" | "json" | "zip"


HOSPITAL_FILES: list[HospitalFileSpec] = [
    # Fill in one entry per Nashville-area hospital after you have confirmed the
    # direct file URL on the hospital's price transparency page. Example shape:
    # HospitalFileSpec(
    #     key="example_hospital_mrf",
    #     hospital_name="EXAMPLE HOSPITAL",
    #     ccn="440000",
    #     url="https://example.org/path/to/standard-charges.csv",
    #     file_format="csv",
    # ),
]


@dataclass
class FileDownloadConfig:
    # Price files can be very large and slow to start, so the read timeout is
    # much longer than the API timeout above.
    connect_timeout_seconds: float = 30.0
    read_timeout_seconds: float = 300.0
    chunk_bytes: int = 1024 * 1024                 # stream in 1 MiB chunks
    max_bytes: int = int(os.environ.get("CMS_MAX_FILE_BYTES", 2 * 1024**3))  # safety cap
    # Some hospital sites return 403 to non-browser User-Agents.
    user_agent: str = "Mozilla/5.0 (compatible; nss-de-final-project/1.0)"
    retry: RetryConfig = field(default_factory=RetryConfig)


FILES = FileDownloadConfig()

# Landing path for hospital price files inside the same Volume as the API data.
PRICE_FILES_ROOT = f"{VOLUME_ROOT}/hospital_price_files"


def apply_widget_overrides(dbutils) -> None:
    """
    Optional helper for notebook use: pulls overrides from Databricks widgets
    so you can re-run ingestion for a different catalog/schema without
    editing this file. Call at the top of a notebook if you want it.
    """
    global CATALOG, BRONZE_SCHEMA, LANDING_VOLUME, VOLUME_ROOT, MANIFEST_TABLE, PRICE_FILES_ROOT
    CATALOG = dbutils.widgets.get("catalog") if _has_widget(dbutils, "catalog") else CATALOG
    BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema") if _has_widget(dbutils, "bronze_schema") else BRONZE_SCHEMA
    LANDING_VOLUME = dbutils.widgets.get("landing_volume") if _has_widget(dbutils, "landing_volume") else LANDING_VOLUME
    VOLUME_ROOT = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{LANDING_VOLUME}"
    MANIFEST_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.ingestion_manifest"
    PRICE_FILES_ROOT = f"{VOLUME_ROOT}/hospital_price_files"


def _has_widget(dbutils, name: str) -> bool:
    try:
        dbutils.widgets.get(name)
        return True
    except Exception:
        return False
