# DECISIONS.md

> **Status key.** **Decided** = made. **Draft — confirm** = current plan, still to verify.
> `TODO` = needs a real value. Delete this note before submitting.

## The question

*By the end of this, I'll be able to answer **how much the negotiated price for hip and knee
replacement varies across Nashville-area hospitals** and **whether that variation is associated
with 30-day readmission performance** about **Nashville-area hospitals**.*

**Scope (Decided).** Narrowed from statewide to Nashville-area to fit two weeks.  Analysis is at the
hospital and payer level.

**Hospitals (Decided).** Four, each with a published price file that passed the outbound-access check.
HCA TriStar hospitals were considered and dropped for time.

| Key | Hospital | Format | Size | `last_updated_on` in file | CCN |
| --- | --- | --- | --- | --- | --- |
| `vumc_mrf` | Vanderbilt University Medical Center | JSON | 1.87 GB | 2025-10-01 | TODO |
| `sthss_mrf` | Saint Thomas Hospital for Specialty Surgery | JSON | 22 MB | 2026-04-14 | TODO (may not be in CMS data) |
| `struth_mrf` | Ascension Saint Thomas Rutherford | CSV, tall | 349 MB | 01/01/2026 | TODO |
| `wmc_mrf` | Williamson Medical Center | CSV, wide | 177 MB | 8/1/2026 | TODO |

- Rutherford's file also covers its Westlawn campus; it is counted once.
- The Specialty Surgery file's `hospital_name` is a legal entity ("Baptist Womens Health Center LLC"),
  but its `location_name` and address (2011 Murphy Ave, Nashville) identify the hospital.

---

## Decision 1 — Source

**Choice (Decided).** Two public sources, joined on CMS Certification Number (CCN):
1. **Hospital price transparency files** (negotiated rates by payer, plan, and code).
2. **CMS Provider Data Catalog:** Unplanned Hospital Visits (hip/knee 30-day readmission),
   Complications and Deaths, the Hospital Readmissions Reduction Program file, and Hospital
   General Information (CCN, county, ownership).

| | Price files | CMS Provider Data Catalog |
| --- | --- | --- |
| URL | `TODO` per hospital (in `config.HOSPITAL_FILES`) | `https://data.cms.gov/provider-data/` |
| Terms | `TODO` | `TODO` |
| Update frequency | `TODO` (each file states its own last-updated date) | `TODO` (see each dataset page) |

**Why.** Negotiated rates exist only in price transparency files. CMS claims data shows Medicare
payments, so it cannot answer the price question. CMS outcome measures are risk-standardized and
keyed by CCN, which makes the join reliable.

**Alternatives.** CMS claims data alone (cannot answer the question); statewide scope (too many
file layouts for two weeks).

**Risks.**
- Four hospitals, four layouts: two nested JSON files, one tall CSV, one wide CSV.
- Many rates are percentages or algorithms, not dollars (for example VUMC's "% of Charge").
- Prices are commercial; outcomes cover Medicare fee-for-service patients, and the periods differ.
- With few hospitals and CMS low-volume suppression, the readmission analysis is **exploratory**.

**Access (Decided).** Databricks Free Edition limits outbound internet. After verifying the account,
CMS and all four hospital sites were reachable.

---

## Decision 2 — Raw landing

**Choice (Decided).** Unity Catalog Volume `main.bronze.raw_landing`; files stored exactly as received.
- CMS pages: `<dataset_key>/run_date=YYYY-MM-DD/page_NNNNN.json`
- Price files: `hospital_price_files/<key>/ingest_date=.../run_id=.../<key>.<format>`

**Why.** It is the platform's own storage, so there is no extra bucket or credentials, and files are
plain paths. Bronze is built from landed files, never from the source, so any run can be replayed.

**Alternatives.** S3-compatible storage such as RustFS (used in class) or GCS: more wiring, no benefit here.

**Ingestion behavior (Decided).**
- Retry with backoff; one failing source never stops the others.
- A manifest table (`main.bronze.ingestion_manifest`) logs every event with bytes, SHA-256, ETag,
  and Last-Modified. Page events are written in batches of 25, not one Delta write per page.
- A partial file is deleted if a download is interrupted.
- **Sanity checks run for every source after each run** (manifest vs. files on disk vs. config); any
  failure fails the run.
- Price files unchanged since the last download (ETag/Last-Modified) are skipped, and refreshed
  after 30 days regardless. Size cap is 5 GiB.

**First full run (2026-10-01).** All 8 sources landed and passed checks in about 29 minutes
(VUMC about 15). Rows: 5,419 hospitals; 67,060 unplanned visits; 95,800 complications; 18,330 HRRP.

**Tradeoffs.** CMS pages are keyed by date, so a same-day rerun overwrites the same files (`TODO`:
add `run_id`?). Tied to Databricks. Older duplicate landings (such as `ast_rutherford_mrf`) can be
deleted.

---

## Decision 3 — Analytical store

**Choice (Decided platform).** Delta tables in Unity Catalog (`main.bronze`; silver and gold schema
names `TODO`).

**Why.** One platform for landing, tables, transformation, and orchestration. Unlike DuckDB there is
no single-writer lock, so no separate serving copy. `dbt-databricks` supports Unity Catalog directly.

**Alternatives.** DuckDB (writer-lock conflict, local only); Postgres (works, but needs Docker and
separate hosting for a scheduled deployment).

**Bronze layout (Decided).** One table per source file, loaded **as-is: every row, no cleaning, no
filtering** (hip/knee filtering happens in silver). Each table has `_source_file`, `_run_id`, and
`_ingested_at`.
- **CMS datasets (4 tables):** all values as text, from the latest run's `succeeded` pages.
- **JSON price files:** streamed into a JSON Lines staging copy, then loaded one row per item in a
  single `raw_json` column. Spark cannot efficiently read one 1.87 GB JSON document.
- **CSV price files:** loaded with their own columns, all text, using Delta column mapping so names
  with spaces and slashes survive. Each file's metadata (hospital name, address, license) goes to a
  `<key>_header` table; JSON files get one too.
- Row counts are reconciled against the source; loads are idempotent; staging copies are deleted.

**Tradeoffs.**
- Free Edition: one 2X-Small SQL warehouse shared by dbt and the dashboard, a daily compute quota,
  and serverless-only compute.
- Spark SQL differs from DuckDB SQL.
- Williamson's wide CSV may have thousands of columns (`TODO`: record the count after loading).
  Unity Catalog column names are limited to about 255 characters.

---

## Decision 4 — Transform + orchestration

**Transform (Decided).** dbt with `dbt-databricks`, developed in VS Code with Git and synced to a
Databricks Git folder.
- **Silver (planned).** Parse the JSON `raw_json` with an explicit schema; unpivot Williamson's wide
  columns **after** filtering to the hip/knee codes (via a dbt macro); standardize payer, plan, code,
  setting, and rate type; filter to CPT 27130 and 27447 (and MS-DRG 469/470 where present); restrict to
  Nashville-area hospitals; add a CCN crosswalk seed.
- **Rate types (`TODO`).** Decide whether percentage and algorithm-based rates are excluded or
  converted to dollars using the gross charge.
- **Gold (planned).** `price variation` (per hospital, code, payer: median, min, max, spread) and
  `price vs. readmission` (per hospital: median rate, readmission rate with confidence interval,
  suppression flag).
- **Automated checks (`TODO`, at least four, all passing).** Candidates: `not_null` on hospital and
  code, `unique` on the silver grain (hospital + code + payer + plan), `accepted_values` on code type,
  `relationships` to the crosswalk, and a range test for implausible rates.

**Orchestration (Draft — confirm).** A Databricks Job: ingest → bronze load → `dbt run` and `dbt test`
→ publish. Retries exist at two levels: HTTP backoff in the code and task retries in the Job.

**Alternatives.** dbt + Prefect (class stack): Python-native flows, but needs a separate host to run
on a schedule.

**Open.** Confirm a dbt task runs on Free Edition (fallback: run dbt from a notebook task); export the
Job definition JSON to the repo; set the schedule early enough to build run history.

---

## Decision 5 — Serving

**Choice (Draft — confirm).** A Databricks dashboard on the gold tables, with two views:
1. Distribution of negotiated rates by hospital for hip and knee replacement, filterable by payer.
2. Price vs. readmission scatter with confidence intervals and a sample-size caveat on the chart.

**Why.** Same platform as gold, so no serving copy or extra service to host.

**Alternatives.** Metabase or Streamlit through the SQL warehouse: more layout control, more setup.

**Open.** Confirm dashboards are available on Free Edition; otherwise use Streamlit or Metabase.

---

## Known limitations

- Prices and outcomes describe different populations and periods, so any relationship is an
  association, not a causal effect.
- Few hospitals plus suppressed low-volume results make the readmission analysis exploratory.
- Teaching status, volume, and case mix may affect both measures; they are shown as context, not modeled.
- Price file quality varies: missing values, placeholder rates, and percentage or algorithm rates are
  flagged in silver, not silently dropped.

## Open items

- [ ] CCNs for all four hospitals; check whether Specialty Surgery has CMS outcome data
- [ ] Source terms, URLs, and update frequency (Decision 1 table)
- [ ] Run the bronze load; record table counts and Williamson's column count
- [ ] Rate-type rule for percentage and algorithm rates
- [ ] dbt task on Free Edition, dashboard tool, and schedule confirmed
- [ ] Final list of four or more passing data-quality checks
- [ ] Job JSON exported to the repo; older duplicate landings deleted
