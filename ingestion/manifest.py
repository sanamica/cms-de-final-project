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

from .config import MANIFEST_TABLE

MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("dataset_key", StringType(), False),
        StructField("source", StringType(), False),       # cms_data_api | provider_data_catalog | hospital_price_file
        StructField("page_number", IntegerType(), True),
        StructField("row_count", IntegerType(), True),
        StructField("status", StringType(), False),        # started | succeeded | failed
        StructField("error_message", StringType(), True),
        StructField("landing_path", StringType(), True),
        StructField("event_ts", TimestampType(), False),
        # Added for hospital price files. Kept at the END so the order matches
        # what ALTER TABLE ... ADD COLUMNS produces on an existing table.
        StructField("file_bytes", LongType(), True),
        StructField("sha256", StringType(), True),
    ]
)

# Columns added after the table was first created (name -> SQL type).
_ADDED_COLUMNS = {"file_bytes": "BIGINT", "sha256": "STRING"}


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


def log_event(
    spark: SparkSession,
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
) -> None:
    """Append a single manifest event. Called for every page, plus run
    start/end, so the manifest table doubles as an audit log and a
    dashboard source ("which hospitals failed, and why")."""
    row = [
        (
            run_id,
            dataset_key,
            source,
            page_number,
            row_count,
            status,
            error_message,
            landing_path,
            datetime.now(timezone.utc),
            file_bytes,
            sha256,
        )
    ]
    spark.createDataFrame(row, MANIFEST_SCHEMA).write.format("delta").mode("append").saveAsTable(MANIFEST_TABLE)


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
