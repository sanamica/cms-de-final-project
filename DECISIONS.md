# DECISIONS.md

## The question

For hip and knee replacement at **four Nashville-area hospitals**:

1. How much does the negotiated price vary **across hospitals, within the same payer category**?
2. How much does it vary **between payer categories within a hospital**?

**Scope:** Narrowed from statewide to the Nashville area to fit two weeks. Analysis is at the
hospital and payer level.

**Hospitals:** Four, each with a published price file that passed the outbound-access check.
HCA TriStar hospitals were considered and dropped for time.

|Key|Hospital|Format|Size|Last updated on|CCN|
|-|-|-|-|-|-|
|`vumc_mrf`|Vanderbilt University Medical Center|CSV, tall|203 MB|2026-10-01|440039|
|`sthss_mrf`|Saint Thomas Hospital for Specialty Surgery|JSON|22 MB|2026-04-14|440218|
|`struth_mrf`|Ascension Saint Thomas Rutherford|CSV, tall|349 MB|2026-01-01|440053|
|`wmc_mrf`|Williamson Medical Center|CSV, wide|177 MB|2026-08-01|440029|

* Rutherford's file also covers its Westlawn campus; it is counted once.
* The Specialty Surgery file's `hospital_name` is a legal entity ("Baptist Womens Health Center LLC"),
  but its `location_name` and address (2011 Murphy Ave, Nashville) identify the hospital.

---

## Decision 1 — Source

**Choice:** Two public sources, joined on CMS Certification Number (CCN):

1. **Hospital price transparency files** (negotiated rates by payer, plan, and code).
2. **CMS Provider Data Catalog:** Hospital General Information (CCN, county, ownership, type). The
   pipeline also lands Unplanned Hospital Visits, Complications and Deaths, and the Hospital
   Readmissions Reduction Program file in bronze; they are not used in this version's analysis.

||Price files|CMS Provider Data Catalog|
|-|-|-|
|URL|VUMC: `https://finance.vumc.org/assets/pub/pt/352528741_vanderbilt-university-medical-center_standardcharges.csv`<br />St. Thomas Specialty: `https://mrfs.hyvehealthcare.com/USPI/621772195_baptist-womens-health-center-llc_standardcharges.json`<br />St. Thomas Rutherford: `https://healthcare.ascension.org/-/media/project/ascension/healthcare/price-transparency-files/tn-csv/620475842_saint-thomas-rutherford-hospital_standardcharges.csv`<br />WMC: `https://williamsonhealth.org/content/uploads/2026/08/621501534_williamson-medical-center_standardcharges.csv`|`https://data.cms.gov/provider-data/`|

**Why.** Negotiated rates exist only in price transparency files. CMS claims data shows Medicare
payments, so it cannot answer the price question. CMS Hospital General Information is keyed by CCN,
which gives every hospital a reliable identifier and its basic attributes.

**Alternatives.** CMS claims data alone (cannot answer the question); statewide scope (too many
file layouts for two weeks).

**Risks.**

* Four hospitals, three layouts: one nested JSON file (Specialty Surgery), two tall CSVs (VUMC and
  Rutherford), and one wide CSV (Williamson). VUMC withdrew its JSON file in October 2026 (the link
  began redirecting to a sign-in page) and now publishes a CSV, which also labels billing class.
* Many rates are percentages or algorithms, not dollars (for example VUMC's "% of Charge").

**Access:** Databricks Free Edition limits outbound internet. After verifying the account,
CMS and all four hospital sites were reachable.

---

## Decision 2 — Raw landing

**Choice:** Unity Catalog Volume `main.bronze.raw_landing`; files stored exactly as received.

* CMS pages: `<dataset_key>/run_date=YYYY-MM-DD/page_NNNNN.json`
* Price files: `hospital_price_files/<key>/ingest_date=.../run_id=.../<key>.<format>`

**Why.** It is the platform's own storage, so there is no extra bucket or credentials, and files are
plain paths. Bronze is built from landed files, never from the source, so any run can be replayed.

**Alternatives.** S3-compatible storage such as RustFS (used in class) or GCS: more wiring, no benefit here.

**Ingestion behavior.**

* Retry with backoff; one failing source never stops the others.
* A manifest table (`main.bronze.ingestion_manifest`) logs every event with bytes, SHA-256, ETag,
  and Last-Modified. Page events are written in batches of 25, not one Delta write per page.
* A partial file is deleted if a download is interrupted. A response that is not the expected file type,
  such as a sign-in page, is rejected and fails the run; this caught VUMC withdrawing its JSON file.
* **Sanity checks run for every source after each run** (manifest vs. files on disk vs. config); any
  failure fails the run.
* Price files unchanged since the last download (ETag/Last-Modified) are skipped, and refreshed
  after 30 days regardless. Size cap is 5 GiB.

**First full run (2026-10-01).** All 8 sources landed and passed checks in about 29 minutes
(VUMC, then still a 1.87 GB JSON file, took about 15). Rows: 5,419 hospitals; 67,060 unplanned
visits; 95,800 complications; 18,330 HRRP.

**Tradeoffs.**

* Tied to Databricks.
* CMS pages are keyed by date, so a same-day rerun overwrites the same files.
* Older duplicate landings (such as `ast_rutherford_mrf`) can be deleted.

---

## Decision 3 — Analytical store

**Choice:** Delta tables in Unity Catalog (`main.bronze`, `main.silver` and `main.gold`).

**Why.** One platform for landing, tables, transformation, and orchestration. Unlike DuckDB there is
no single-writer lock, so no separate serving copy. `dbt-databricks` supports Unity Catalog directly.

**Alternatives.** DuckDB (writer-lock conflict, local only); Postgres (works, but needs Docker and
separate hosting for a scheduled deployment).

**Bronze layout.** One table per source file, loaded **as-is: every row, no cleaning, no
filtering** (hip/knee filtering happens in silver). Each table has `_source_file`, `_run_id`, and
`_ingested_at`.

* **CMS datasets (4 tables):** all values as text, from the latest run's `succeeded` pages.
* **JSON price files:** streamed into a JSON Lines staging copy, then loaded one row per item in a
  single `raw_json` column. Spark cannot efficiently read one very large JSON document (VUMC's was
  1.87 GB before it switched to CSV).
* **CSV price files:** loaded with their own columns, all text, using Delta column mapping so names
  with spaces and slashes survive. Each file's metadata (hospital name, address, license) goes to a
  `<key>_header` table; JSON files get one too.
* Row counts are reconciled against the source; loads are idempotent; staging copies are deleted.
* **First load (2026-10-02):** all 8 tables loaded and reconciled against their sources: the four CMS
  tables, Rutherford (1,469,297 rows), Williamson (94,162), VUMC (96,417 items from its earlier JSON
  file, 65 s including the JSON-to-JSON Lines staging), and Specialty Surgery (16,710 items).

**Tradeoffs.**

* Free Edition: one 2X-Small SQL warehouse shared by dbt and the dashboard, a daily compute quota,
  and serverless-only compute.
* Spark SQL differs from DuckDB SQL.
* Williamson's wide CSV loads as 409 columns (406 from the file plus 3 lineage columns) with Delta
  column mapping on. Unity Catalog column names are limited to about 255 characters.

---

## Decision 4 — Transform + orchestration

**Transform:** dbt with `dbt-databricks`, developed in VS Code with Git and synced to a
Databricks Git folder.

* **Silver (`silver_negotiated_rates`):** One row per hospital, billing code, setting,
  payer, and plan. JSON `raw_json` is parsed with an explicit schema; Williamson's wide columns are
  unpivoted by a dbt macro; all four hospitals share one schema. **Filtered to hip and knee
  replacement:** CPT 27130 and 27447, and MS-DRG 469 and 470 (the codes live in the
  `procedure_codes` seed, so adding a procedure is a one-line change). A dbt variable,
  `procedure_scope: all`, keeps every procedure instead, at a much larger size and run time. Each row
  carries its price file's last-updated date (`file_last_updated_on`, read from the file's own
  header). Scope comes from the four ingested files and a CCN crosswalk seed, so no county filter is
  needed. Silver models are views except this table, which is materialized because parsing the JSON
  file and unpivoting Williamson's columns is too slow to repeat on every query.
* **Row identity and billing class.** Each silver row carries the item's full code list
  (`item_codes`, including the hospital's own chargemaster code) and `modifiers`, so separate
  chargemaster lines that share one CPT code are not mistaken for duplicates; rows that repeat exactly
  are removed. Gold groups by **billing class** so facility and professional (surgeon) fees are never
  pooled. Evidence: Williamson labels both, and for CPT 27447 its facility median is $12,569 (32 rates)
  against $1,244 for professional (58 rates); pooled, the professional rows set the median. VUMC's CSV
  labels billing class as well. Rutherford does not, so it gets a documented assumption from the
  crosswalk seed: it is treated as facility (flagged `billing_class_assumed`; its amounts are
  consistent with facility rates).
* **Code family:** Hospitals label the same code differently: VUMC lists CPT 27130 and 27447
  as HCPCS, the others as CPT. Silver adds `code_family` (CPT and HCPCS together), and gold compares by
  it; otherwise VUMC sat alone in its own group and could not be compared.
* **Payer categories:** A hospital's median across all payers depends on how many Medicaid and Medicare
  Advantage plans it lists, because those pay far less than commercial plans (Specialty Surgery's rates
  run from $584 for a TennCare plan to $21,687 for commercial), so a hospital's overall median is not a
  fair price to compare. Each payer/plan is sorted into commercial, exchange, Medicare Advantage,
  Medicaid, or other government by keyword rules in a seed (`payer_category_rules.csv`); names that match
  no rule default to commercial and are flagged (`payer_category_is_default`). On the real payer list,
  41 hand-checked names all landed in the intended category; about 40% of dollar-rate lines are
  commercial by default, so the rules should be reviewed if commercial results look off. The
  `Amerivantage` and `Network E` (exchange) rules are assumptions, and BlueCare and Bluegrass Family
  Health are mapped to Medicaid after they showed up as low-paying commercial plans. Three gold models
  use the categories: `gold_price_by_payer_category`, `gold_hospital_comparison_by_payer_category`
  (Question 1, within commercial plans only), and `gold_payer_category_gap` (Question 2: each
  category's median against the commercial median, per hospital and service).
* **Rate types:** The main comparison uses **dollar rates only**. Percentage and algorithm
  rates stay in silver but are excluded from the statistics and counted per hospital and code, and
  placeholder dollar rates (under $1, such as $0.01) are excluded and counted too. Converting
  percentages to dollars was rejected: the base for "% of billed charges" is the whole bill, gross
  charges are sometimes missing, and carve-outs are not captured. Rutherford's dollar amounts for
  percent-of-charge contracts are calculated by the hospital, so they count as dollar rates; the
  `methodology` column keeps that visible.
* **Gold:** `gold_price_variation` (per hospital, code, setting, and billing class: median, quartiles,
  min, max, spread, and counts of excluded rates; each payer/plan is first collapsed to one rate, the
  median of its dollar lines without a billing modifier, so a payer listed on several lines does not
  outweigh one listed once; a modifier marks a variant such as a reduced service, and Rutherford lists
  the same CPT code at $6,105 and $22,577 for one plan); `gold_price_variation_across_hospitals`
  (spread of hospital medians for the same code, setting, and billing class; a hospital counts only
  with at least five payer/plans that have a dollar rate, set by the `min_payer_plans_for_comparison`
  variable).
* **Hospital dimension (`dim_hospital`):** One row per hospital, keyed by CCN: the crosswalk (which
  price file belongs to which hospital) joined to CMS Hospital General Information (city, county,
  ownership, type), and the price file's last-updated date and name as stated in the file. Gold tables
  reference it for hospital attributes, and `relationships` tests point at it.
* **Automated checks:** `not_null` and `relationships` to the crosswalk on hospital CCN,
  `accepted_values` on rate type, `unique` and `not_null` on the crosswalk and CMS tables, singular
  tests for negative dollar rates and for inconsistent min/median/max, and a uniqueness check on the
  silver grain that only warns. Rate type and methodology are part of that grain, because VUMC lists a
  payer/plan on separate dollar and algorithm lines.
* **Test results.** The first run on Databricks built 12 models in 48 seconds; 34 of 35 tests passed,
  with one warning (154 duplicate groups on the silver grain). After the VUMC CSV switch the warning
  showed 145 groups: 139 from VUMC (85 with mixed dollar and algorithm lines, 78 with different dollar
  amounts) and 6 from Specialty Surgery; Rutherford and Williamson had none. The warning is kept as
  documentation; gold collapses each payer/plan to one rate, so these groups do not double-count.

**Orchestration:** One Databricks Job, `cms_hospital_pricing_pipeline`, with three tasks in a
chain: `ingest` (notebook) → `bronze_load` (notebook) → `dbt_build` (dbt task: `dbt seed`, `dbt run`,
`dbt test`; catalog `main`, schema `silver`). Settings: 2 task retries, one concurrent run, job
parameter `failure_test` (default `no`), and a schedule every 6 hours (America/Chicago). A full run
takes about 18–23 minutes. Retries exist at two levels: HTTP backoff with a logged line per retry in
the code, and task retries in the Job. The dbt task works on Free Edition. The dashboard reads the gold
tables after each run, so there is no separate publish task.

**Run history (evidence).** The run history shows repeated green scheduled runs (Oct 14 and Oct 15),
plus the failures that taught us something: early `dbt_build` failures because the seed CSVs were
ignored by Git and never reached the workspace (fixed by force-adding the three seed files), and the
deliberate failure run below.

**Deliberate failure test.** The ingest notebook reads a job parameter, `failure_test` (default `no`).
Run with `yes`, it points one price file at a host that does not exist and runs only the price files,
so the download retries with backoff (each retry is logged), the failure is recorded in the manifest,
the sanity check fails the task within a few minutes, and the later tasks are skipped. Nothing in the
data changes, and the next normal run recovers.

**Result.** The manual run on Oct 14 with `failure_test = yes` failed in 4 min 15 s with a
`RunExecutionError` at `ingest`, after the logged retries; `bronze_load` and `dbt_build` did not run.
The next scheduled runs, with the default `no`, succeeded end to end, so the pipeline recovered without
any manual repair of data. The run-history screenshot in the deck shows the failure followed by green
runs.

**Alternatives.** dbt + Prefect (class stack): Python-native flows, but needs a separate host to run
on a schedule.

---

## Decision 5 — Serving

**Choice:** A published Databricks dashboard, "Knee and Hip replacement commercial pricing in Nashville
area hospitals", on the gold tables, refreshed on the Job's schedule. Two tabs:

1. **Median Price Hip and Knee.** A bar chart of the median commercial outpatient facility rate for hip
   and knee replacement by hospital (St. Thomas Rutherford about $22.9K, VUMC about $21.1K, Williamson
   about $20–21K), a detail table (median, 25th and 75th percentile, min, max, number of payer plans),
   and a caveats note.
2. **Payer Gap Knee Replacement.** The spread between payer categories within each hospital, for knee
   replacement only.

The caveats note on the dashboard says: dollar-amount rates only, facility fees only, outpatient
setting, payer categories assigned by keyword rules, and Specialty Surgery excluded because it labels
its setting "both" and its rates look unusually low and are unverified.

**Why.** Same platform as gold, so no serving copy or extra service to host. Dashboards are available
on Free Edition.

**Alternatives.** Metabase or Streamlit through the SQL warehouse: more layout control, more setup.

---

## Known limitations

* Four hospitals (three on the dashboard) describe patterns, not statistically supported differences.
* Negotiated rates are what hospitals publish, not what patients or plans actually paid.
* Payer categories come from keyword rules; about 40% of dollar-rate lines are commercial by default.
* Rutherford's billing class is assumed to be facility, not stated in its file.
* Price file quality varies: missing values, placeholder rates, and percentage or algorithm rates are
  flagged in silver, not silently dropped.
* Hospital file links change over time and must be updated in `ingestion/config.py` when they do.
