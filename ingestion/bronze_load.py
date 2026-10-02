"""
Bronze load: raw landing (Volume) -> Delta tables in main.bronze.

Rules this module follows (they come from the assignment's bronze definition):
  * One table per source file, loaded as-is: every row, every column, no
    cleaning and NO FILTERING. Hip/knee filtering happens later, in dbt silver.
  * Every table gets three lineage columns: _source_file, _run_id, _ingested_at.
  * Idempotent: a table is rebuilt from scratch, or skipped when it was already
    built from the latest landed run.
  * Each table's row count is reconciled against the source; a mismatch raises.

What is loaded, and how
  CMS datasets (JSON pages)   -> main.bronze.<key>
        Only the pages the manifest marks 'succeeded' for the latest run are read.
        All values are loaded as strings.
  Price file, JSON (VUMC...)  -> main.bronze.<key>          one row per item, one column
                                 main.bronze.<key>_header   `raw_json` holding the item verbatim
        The file is streamed (ijson) into a JSON Lines staging copy first,
        because Spark cannot read one 1-2 GB JSON document efficiently. Silver
        parses `raw_json` with an explicit schema.
  Price file, CSV             -> main.bronze.<key>          the file's own columns, all strings
                                 main.bronze.<key>_header   (Delta column mapping keeps the original
                                                             names, including spaces and slashes)
        CMS CSVs start with two metadata lines (names, then values) before the
        real header row. Those two lines go to <key>_header as name/value rows,
        and the rest of the file is loaded as the data table.

The staging copies live under <volume>/bronze_staging/ and are deleted after a
successful load. The original landed files are never modified.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
import shutil
import time

from . import config
from .config import DatasetSpec, HospitalFileSpec
from .manifest import last_download

logger = logging.getLogger("cms_ingestion.bronze")

MAX_COLUMN_NAME = 255          # Unity Catalog limit on column name length, as far as I know
_COPY_CHUNK = 8 * 1024 * 1024


class BronzeLoadError(Exception):
    """A table could not be loaded, or its row count does not match the source."""


# ---------------------------------------------------------------------------
# Table I/O (kept in three small functions so they are easy to test)
# ---------------------------------------------------------------------------
def _table(name: str) -> str:
    return f"{config.CATALOG}.{config.BRONZE_SCHEMA}.{name}"


def _write_table(df, table: str, column_mapping: bool = False) -> None:
    """Rebuild a table from a DataFrame. Dropping first makes this a clean
    create (column mapping cannot be switched on for a table that already exists)."""
    df.sparkSession.sql(f"DROP TABLE IF EXISTS {table}")
    writer = df.write.format("delta").mode("overwrite")
    if column_mapping:
        writer = (
            writer.option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
        )
    writer.saveAsTable(table)


def _table_count(spark, table: str) -> int:
    return spark.table(table).count()


def _loaded_run_id(spark, table: str) -> str | None:
    """The run_id a table was built from (tables are rebuilt whole, so one run_id), or None."""
    try:
        rows = spark.sql(f"SELECT _run_id FROM {table} LIMIT 1").collect()
    except Exception:  # noqa: BLE001 - table does not exist yet
        return None
    return rows[0][0] if rows else None


def _with_lineage(df, source_file, run_id: str):
    from pyspark.sql import functions as F

    src = F.lit(source_file) if isinstance(source_file, str) else source_file
    return (
        df.withColumn("_source_file", src)
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def _check_count(table: str, expected: int, got: int) -> None:
    if expected != got:
        raise BronzeLoadError(f"{table}: {got} rows in the table, {expected} expected from the source")


# ---------------------------------------------------------------------------
# CMS datasets
# ---------------------------------------------------------------------------
def _latest_cms_pages(spark, key: str) -> tuple[str | None, list[str], int]:
    """(run_id, page paths, expected row count) for the latest run that landed pages."""
    pages = (
        f"FROM {config.MANIFEST_TABLE} WHERE dataset_key = '{key}' "
        "AND status = 'succeeded' AND page_number IS NOT NULL"
    )
    rows = spark.sql(
        f"SELECT run_id, landing_path, row_count {pages} "
        f"AND run_id = (SELECT run_id {pages} ORDER BY event_ts DESC LIMIT 1)"
    ).collect()
    if not rows:
        return None, [], 0
    return rows[0]["run_id"], sorted(r["landing_path"] for r in rows), sum(r["row_count"] or 0 for r in rows)


def load_cms_dataset(spark, spec: DatasetSpec, force: bool = False) -> int | str:
    run_id, paths, expected = _latest_cms_pages(spark, spec.key)
    if not paths:
        raise BronzeLoadError(f"{spec.key}: no succeeded pages in the manifest. Run ingestion first.")
    table = _table(spec.key)
    if not force and _loaded_run_id(spark, table) == run_id:
        logger.info("%s already built from run %s; skipping", table, run_id)
        return "skipped"

    from pyspark.sql import functions as F

    df = (
        spark.read.option("multiLine", True)
        .option("primitivesAsString", True)          # keep every value as text
        .json(paths)
    )
    df = _with_lineage(df, F.col("_metadata.file_path"), run_id)
    _write_table(df, table)
    got = _table_count(spark, table)
    _check_count(table, expected, got)
    return got


# ---------------------------------------------------------------------------
# Price files: staging helpers
# ---------------------------------------------------------------------------
def _run_id_from_path(path: str) -> str:
    match = re.search(r"run_id=([^/]+)", path)
    return match.group(1) if match else "unknown"


def _unique_names(names: list[str]) -> list[str]:
    """Blank names become _c<i>; repeated names get a __2, __3... suffix."""
    seen: dict[str, int] = {}
    out = []
    for i, raw in enumerate(names):
        name = (raw or "").strip() or f"_c{i}"
        seen[name] = seen.get(name, 0) + 1
        out.append(name if seen[name] == 1 else f"{name}__{seen[name]}")
    return out


def _stage_csv(src: str, staging: str):
    """Split a CMS-style CSV into (metadata name/value pairs, data-only CSV, data row count, column names)."""
    out = f"{staging}/data.csv"
    with open(src, "rb") as fin:
        line1, line2 = fin.readline(), fin.readline()
        data_start = fin.tell()
        header_line = fin.readline()
        if not line1.lstrip(b"\xef\xbb\xbf").lower().startswith(b"hospital_name") \
                or b"description" not in header_line.lower():
            raise BronzeLoadError(
                f"{os.path.basename(src)} does not have the expected layout: two metadata lines "
                "(starting with hospital_name) followed by a header row containing 'description'."
            )
        fin.seek(data_start)
        with open(out, "wb") as fout:
            shutil.copyfileobj(fin, fout, _COPY_CHUNK)

    meta_names = next(csv.reader([line1.decode("utf-8-sig")]))
    meta_values = next(csv.reader([line2.decode("utf-8-sig")]))
    meta_values += [""] * (len(meta_names) - len(meta_values))
    header_rows = [(i, n, v) for i, (n, v) in enumerate(zip(meta_names, meta_values)) if n.strip()]

    names = _unique_names(next(csv.reader([header_line.decode("utf-8-sig")])))
    too_long = [n[:60] + "..." for n in names if len(n) > MAX_COLUMN_NAME]
    if too_long:
        raise BronzeLoadError(f"{len(too_long)} column name(s) exceed {MAX_COLUMN_NAME} characters, e.g. {too_long[0]}")

    with open(out, newline="", encoding="utf-8-sig") as fh:
        csv.field_size_limit(10_000_000)
        records = sum(1 for _ in csv.reader(fh)) - 1          # minus the header row
    return header_rows, out, records, names


def _stage_json(src: str, staging: str):
    """Stream a CMS-style JSON file into JSON Lines (one standard_charge_information
    item per line) without loading the whole document into memory."""
    import ijson

    marker = b'"standard_charge_information"'
    buf, idx = b"", -1
    with open(src, "rb") as fh:
        while idx == -1 and len(buf) < 64 * 1024 * 1024:
            chunk = fh.read(4 * 1024 * 1024)
            if not chunk:
                break
            buf += chunk
            idx = buf.find(marker)
    if idx == -1:
        raise BronzeLoadError(f"{os.path.basename(src)}: 'standard_charge_information' not found in the first 64 MB")
    header = json.loads(buf[:idx].decode("utf-8-sig").rstrip().rstrip(",") + "}")
    if "hospital_name" not in header:
        raise BronzeLoadError(
            f"{os.path.basename(src)}: the header fields come after the items array, which this loader does not support"
        )

    out, count = f"{staging}/items.jsonl", 0
    with open(src, "rb") as fin, open(out, "w", encoding="utf-8", buffering=1024 * 1024) as fout:
        for item in ijson.items(fin, "standard_charge_information.item", use_float=True):
            fout.write(json.dumps(item, separators=(",", ":"), ensure_ascii=False))
            fout.write("\n")
            count += 1

    header_rows = [
        (i, name, value if isinstance(value, str) else json.dumps(value, ensure_ascii=False))
        for i, (name, value) in enumerate(header.items())
    ]
    return header_rows, out, count


def _header_df(spark, header_rows, source_file: str, run_id: str):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("position", IntegerType()),
        StructField("header_name", StringType()),
        StructField("header_value", StringType()),
    ])
    return _with_lineage(spark.createDataFrame(header_rows, schema), source_file, run_id)


# ---------------------------------------------------------------------------
# Price files
# ---------------------------------------------------------------------------
def load_price_file(spark, spec: HospitalFileSpec, force: bool = False, keep_staging: bool = False) -> int | str:
    prev = last_download(spark, spec.key)
    if not prev:
        raise BronzeLoadError(f"{spec.key}: no successful download in the manifest. Run ingestion first.")
    src = prev["landing_path"]
    run_id = _run_id_from_path(src)
    table = _table(spec.key)
    if not force and _loaded_run_id(spark, table) == run_id:
        logger.info("%s already built from run %s; skipping", table, run_id)
        return "skipped"
    if spec.file_format not in ("csv", "json"):
        raise BronzeLoadError(f"{spec.key}: '{spec.file_format}' price files are not supported by the bronze load yet")

    staging = f"{config.VOLUME_ROOT}/bronze_staging/{spec.key}/{run_id}"
    shutil.rmtree(staging, ignore_errors=True)
    os.makedirs(staging)

    if spec.file_format == "csv":
        header_rows, data_path, expected, names = _stage_csv(src, staging)
        df = (
            spark.read.option("header", True).option("multiLine", True).option("escape", '"')
            .csv(data_path)
        )
        if len(df.columns) != len(names):
            raise BronzeLoadError(f"{spec.key}: Spark found {len(df.columns)} columns, the header has {len(names)}")
        df = df.toDF(*names)
        column_mapping = True          # original names contain spaces, slashes and pipes
    else:
        header_rows, data_path, expected = _stage_json(src, staging)
        df = spark.read.text(data_path).withColumnRenamed("value", "raw_json")
        column_mapping = False

    _write_table(_with_lineage(df, src, run_id), table, column_mapping)
    _write_table(_header_df(spark, header_rows, src, run_id), _table(f"{spec.key}_header"))
    got = _table_count(spark, table)
    _check_count(table, expected, got)

    if not keep_staging:
        shutil.rmtree(staging, ignore_errors=True)
    return got


# ---------------------------------------------------------------------------
def load_all(spark, force: bool = False, keep_staging: bool = False) -> dict[str, int | str]:
    """Load every bronze table. One table failing does not stop the others;
    a summary prints at the end and an error is raised if any table failed."""
    results: dict[str, int | str] = {}
    jobs = [(s.key, lambda s=s: load_cms_dataset(spark, s, force)) for s in config.DATASETS]
    jobs += [(s.key, lambda s=s: load_price_file(spark, s, force, keep_staging)) for s in config.HOSPITAL_FILES]

    timings: dict[str, float] = {}
    for key, job in jobs:
        started = time.monotonic()
        try:
            results[key] = job()
        except Exception as exc:  # noqa: BLE001 - report every table, then fail once
            logger.exception("Bronze load failed for %s", key)
            results[key] = f"FAILED: {type(exc).__name__}: {exc}"
        timings[key] = time.monotonic() - started

    print("\nBronze load summary")
    for key, outcome in results.items():
        print(f"  {key:34} {str(outcome):>12}  ({timings[key]:.0f} s)")
    if any(str(v).startswith("FAILED") for v in results.values()):
        raise BronzeLoadError("One or more bronze tables failed (see summary above).")
    return results
