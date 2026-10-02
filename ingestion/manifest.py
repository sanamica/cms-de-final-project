"""
Ingestion manifest: a Delta table logging every run, at the page level, so
failures are queryable data rather than something buried in job logs.

Design choice: one row per (run_id, dataset_key, page_number). A failed page
gets logged with status="failed" and an error message instead of crashing
the whole run — a partial, well-documented ingestion beats a job that dies
on hospital #7 of 12 and leaves you with nothing.

Requires a live SparkSession, passed in explicitly (don't rely on notebook
globals — makes this testable and reusable from a Job as well as a notebook).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    IntegerType,
)

from .config import MANIFEST_BATCH_SIZE, MANIFEST_TABLE

MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset_key", StringType(), False),
        StructField("source", StringType(), False),       # cms_data_api | provider_data_catalog | hospital_price_file
        StructField("page_number", IntegerType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("status", StringType(), False),        # started | succeeded | failed | skipped
        StructField("error_message", StringType(), True),
        StructField("landing_path", StringType(), True),
        StructField("event_ts", TimestampType(), False),
        # Added for hospital price files. Kept at the END so the order matches
        # what ALTER TABLE ... ADD COLUMNS produces on an existing table.
        StructField("file_bytes", LongType(), True),
        StructField("sha256", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("last_modified", StringType(), True),
        StructField("note", StringType(), True),
    ]
)

# Columns added after the table was first created (name -> SQL type).
_ADDED_COLUMNS = {
    "file_bytes": "BIGINT", "sha256": "STRING",
    "etag": "STRING", "last_modified": "STRING", "note": "STRING",
}


def new_run_id() -> str:
    return str(uuid.uuid4())


def ensure_manifest_table(spark: SparkSession) -> None:
    """Create the manifest table if it doesn't exist yet, and add any columns
    introduced after it was first created. Idempotent."""
    if not _table_exists(spark, MANIFEST_TABLE):
        spark.createDataFrame([], MANIFEST_SCHEMA).writeTo(MANIFEST_TABLE).createOrReplace()
        return
    existing = {f.name for f in spark.table(MANIFEST_TABLE).schema.fields}
    missing = {name: sql_type for name, sql_type in _ADDED_COLUMNS.items() if name not in existing}
    if missing:
        cols = ", ".join(f"{name} {sql_type}" for name, sql_type in missing.items())
        spark.sql(f"ALTER TABLE {MANIFEST_TABLE} ADD COLUMNS ({cols})")


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def _row(
    run_id: str,
    dataset_key: str,
    source: str,
    status: str,
    page_number: int | None = None,
    row_count: int | None = None,
    error_message: str | None = None,
    landing_path: str | None = None,
    file_bytes: int | None = None,
    sha256: str | None = None,
    etag: str | None = None,
    last_modified: str | None = None,
    note: str | None = None,
) -> tuple:
    """One manifest row, in MANIFEST_SCHEMA column order. event_ts is stamped now,
    i.e. when the event happened, even if the row is written to Delta later."""
    return (
        run_id, dataset_key, source, page_number, row_count, status, error_message,
        landing_path, datetime.now(timezone.utc), file_bytes, sha256, etag, last_modified, note,
    )


def _write_rows(spark: SparkSession, rows: list[tuple]) -> None:
    spark.createDataFrame(rows, MANIFEST_SCHEMA).write.format("delta").mode("append").saveAsTable(MANIFEST_TABLE)


def log_event(spark: SparkSession, run_id: str, dataset_key: str, source: str, status: str, **fields) -> None:
    """Append a single manifest event immediately. Use for run-level events
    (started, skipped, price-file outcomes). For many page events, use EventBuffer.

    `fields` may include page_number, row_count, error_message, landing_path,
    file_bytes, sha256, etag, last_modified, note."""
    _write_rows(spark, [_row(run_id, dataset_key, source, status, **fields)])


class EventBuffer:
    """Collects manifest events in memory and writes them in batches.

    One Delta append per page made CMS ingestion slow (about 2 seconds per page).
    Buffering cuts that to one append per `batch_size` events plus one final
    flush. Always call flush() in a `finally:` so events are written even if the
    run fails or is interrupted.

    Tradeoff: if the process is killed outright (not a normal exception or
    interrupt), up to batch_size - 1 buffered events are lost, even though their
    page files are on disk. The sanity checks flag that as a gap or stale file.
    """

    def __init__(self, spark: SparkSession, batch_size: int | None = None):
        self.spark = spark
        self.batch_size = batch_size or MANIFEST_BATCH_SIZE
        self._rows: list[tuple] = []

    def add(self, run_id: str, dataset_key: str, source: str, status: str, **fields) -> None:
        self._rows.append(_row(run_id, dataset_key, source, status, **fields))
        if len(self._rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._rows:
            return
        _write_rows(self.spark, self._rows)   # keep the rows if the write raises
        self._rows = []


def last_download(spark: SparkSession, dataset_key: str) -> dict | None:
    """The most recent successful download of a price file (the file bronze should
    read), or None if it has never been downloaded. Skipped runs are not downloads."""
    rows = spark.sql(
        "SELECT landing_path, file_bytes, sha256, etag, last_modified, event_ts "
        f"FROM {MANIFEST_TABLE} WHERE dataset_key = '{dataset_key}' "
        "AND source = 'hospital_price_file' AND status = 'succeeded' "
        "ORDER BY event_ts DESC LIMIT 1"
    ).collect()
    return rows[0].asDict() if rows else None


def run_summary(spark: SparkSession, run_id: str):
    """Convenience query: pull back everything logged for one run, useful
    at the end of a notebook cell to eyeball what happened."""
    return spark.sql(
        f"""
        SELECT dataset_key, source, status, count(*) AS events,
               sum(coalesce(row_count, 0)) AS total_rows,
               sum(coalesce(file_bytes, 0)) AS total_bytes
        FROM {MANIFEST_TABLE}
        WHERE run_id = '{run_id}'
        GROUP BY dataset_key, source, status
        ORDER BY dataset_key, status
        """
    )
