# CMS Ingestion Module

Ingests both CMS open-data APIs into a Unity Catalog Volume, with retry/backoff
and a per-page Delta manifest log. No S3 or GCS — Databricks Volumes are the
landing zone.

## Layout

```
ingestion/
  config.py                  # UC paths, API bases, retry settings, dataset registry
  http_client.py              # shared GET with retry + exponential backoff + jitter
  cms_data_api.py              # data.cms.gov main API client (size/offset paging)
  provider_data_catalog.py     # Provider Data Catalog client (limit/offset, results-wrapped)
  manifest.py                  # Delta manifest table: run/page-level log
  run_ingest.py                 # orchestrator — run this
```

## Before your first run

1. Create the catalog/schema/volume in Databricks (adjust names to taste):
   ```sql
   CREATE CATALOG IF NOT EXISTS main;
   CREATE SCHEMA IF NOT EXISTS main.bronze;
   CREATE VOLUME IF NOT EXISTS main.bronze.raw_landing;
   ```
   These names must match `config.py`'s `CATALOG` / `BRONZE_SCHEMA` /
   `LANDING_VOLUME` (or override via env vars / widgets).

2. Fill in `config.DATASETS` with the dataset UUIDs/IDs you actually want.
   - For the main API: browse data.cms.gov, the dataset UUID is in its URL.
   - For the Provider Data Catalog: run `provider_data_catalog.list_datasets()`
     once to see everything available, then hardcode the IDs you're keeping.

3. `pip install httpx` on the cluster (or add to a cluster library / job
   environment) — `pyspark` is already provided by the Databricks runtime.

## Running

In a notebook cell:
```python
%pip install httpx
from ingestion.run_ingest import main
run_id = main(spark)
```

As a Databricks Job task (Python script), point the task at `run_ingest.py`
directly — it picks up the notebook-provided `spark` global at the bottom of
the file.

## Checking what happened

```sql
SELECT * FROM main.bronze.ingestion_manifest
WHERE run_id = '<run_id>'
ORDER BY event_ts;

-- Failures only
SELECT dataset_key, page_number, error_message, event_ts
FROM main.bronze.ingestion_manifest
WHERE status = 'failed'
ORDER BY event_ts DESC;
```

That failures query is the thing to screenshot for the "data quality
limitations" section of your final writeup — a visible, queryable log of what
didn't come through beats a pipeline that just silently drops rows.

## Notes on the two APIs

- **data.cms.gov main API**: `size`/`offset` paging, max 5000 rows/page,
  returns a bare JSON list. Optional exact-match column filters via
  `filter[COLUMN]=VALUE`.
- **Provider Data Catalog**: a *different* system — `limit`/`offset` paging,
  rows come wrapped in a `results` key, and in practice holds up better at a
  few hundred rows per page than at the main API's max. Don't assume the two
  clients are interchangeable; they aren't.

## Extending

- Column filters beyond exact-match (e.g. `CONTAINS`, ranges) — extend
  `cms_data_api.fetch_dataset_pages` to accept a pre-built params dict.
- Bronze Delta tables over the landed JSON — use Auto Loader
  (`cloudFiles` format) pointed at `VOLUME_ROOT/<dataset_key>/`, or a
  scheduled `COPY INTO` per dataset. That's Week 1's second half.
