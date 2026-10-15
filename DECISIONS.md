# DECISIONS.md

## The question

*By the end of this, I'll be able to answer **how much the negotiated price for hip and knee
replacement varies across Nashville-area hospitals, within the same payer category** and <b>how
much it varies between payer categories within a hospital</b> about <b>four Nashville-area
hospitals</b>?*

**Scope :** Narrowed from statewide to Nashville-area to fit two weeks. Analysis is at the
hospital and payer level.

**Hospitals:** Four, each with a published price file that passed the outbound-access check.
HCA TriStar hospitals were considered and dropped for time.Specialty Surgery has no CMS readmission data, which leaves
three hospitals, too few to test an association.

|Key|Hospital|Format|Size|`last\\\_updated\\\_on` in file|CCN|
|-|-|-|-|-|-|
|`vumc\\\_mrf`|Vanderbilt University Medical Center|CSV, tall|203 MB|2026-10-01|440039|
|`sthss\\\_mrf`|Saint Thomas Hospital for Specialty Surgery|JSON|22 MB|2026-04-14|440218|
|`struth\\\_mrf`|Ascension Saint Thomas Rutherford|CSV, tall|349 MB|01/01/2026|440053|
|`wmc\\\_mrf`|Williamson Medical Center|CSV, wide|177 MB|8/1/2026|440029|

* Rutherford's file also covers its Westlawn campus; it is counted once.
* The Specialty Surgery file's `hospital\\\_name` is a legal entity ("Baptist Womens Health Center LLC"),
but its `location\\\_name` and address (2011 Murphy Ave, Nashville) identify the hospital.

\---

## Decision 1 — Source

**Choice:** Two public sources, joined on CMS Certification Number (CCN):

1. **Hospital price transparency files** (negotiated rates by payer, plan, and code).
2. **CMS Provider Data Catalog:** Unplanned Hospital Visits (hip/knee 30-day readmission),
Complications and Deaths, the Hospital Readmissions Reduction Program file, and Hospital
General Information (CCN, county, ownership).

||Price files|CMS Provider Data Catalog|
|-|-|-|
|URL|`TODO` per hospital (in `config.HOSPITAL\\\_FILES`)|`https://data.cms.gov/provider-data/`|
|Terms|`TODO`|`TODO`|
|Update frequency|`TODO` (each file states its own last-updated date)|`TODO` (see each dataset page)|

**Why.** Negotiated rates exist only in price transparency files. CMS claims data shows Medicare
payments, so it cannot answer the price question. CMS outcome measures are risk-standardized and
keyed by CCN, which makes the join reliable.

**Alternatives.** CMS claims data alone (cannot answer the question); statewide scope (too many
file layouts for two weeks).

**Risks.**

* Four hospitals, three layouts: one nested JSON file (Specialty Surgery), two tall CSVs (VUMC and
Rutherford), and one wide CSV (Williamson). VUMC withdrew its JSON file in October 2026 (the link
began redirecting to a sign-in page) and now publishes a CSV, which also labels billing class.
* Many rates are percentages or algorithms, not dollars (for example VUMC's "% of Charge").
* Prices are commercial; outcomes cover Medicare fee-for-service patients, and the periods differ.
* With few hospitals and CMS low-volume suppression, the readmission analysis is **exploratory**.
Specialty Surgery is in CMS general information but has no hip/knee readmission or complication
data, so that analysis covers three hospitals (VUMC, Rutherford, Williamson).

**Access:** Databricks Free Edition limits outbound internet. After verifying the account,
CMS and all four hospital sites were reachable.

\---

## Decision 2 — Raw landing

**Choice:** Unity Catalog Volume `main.bronze.raw\\\_landing`; files stored exactly as received.

* CMS pages: `<dataset\\\_key>/run\\\_date=YYYY-MM-DD/page\\\_NNNNN.json`
* Price files: `hospital\\\_price\\\_files/<key>/ingest\\\_date=.../run\\\_id=.../<key>.<format>`

**Why.** It is the platform's own storage, so there is no extra bucket or credentials, and files are
plain paths. Bronze is built from landed files, never from the source, so any run can be replayed.

**Alternatives.** S3-compatible storage such as RustFS (used in class) or GCS: more wiring, no benefit here.

**Ingestion behavior.**

* Retry with backoff; one failing source never stops the others.
* A manifest table (`main.bronze.ingestion\\\_manifest`) logs every event with bytes, SHA-256, ETag,
and Last-Modified. Page events are written in batches of 25, not one Delta write per page.
* A partial file is deleted if a download is interrupted. A response that is not the expected file type,
such as a sign-in page, is rejected and fails the run; this caught VUMC withdrawing its JSON file.
* **Sanity checks run for every source after each run** (manifest vs. files on disk vs. config); any
failure fails the run.
* Price files unchanged since the last download (ETag/Last-Modified) are skipped, and refreshed
after 30 days regardless. Size cap is 5 GiB.

**First full run (2026-10-01).** All 8 sources landed and passed checks in about 29 minutes
(VUMC about 15). Rows: 5,419 hospitals; 67,060 unplanned visits; 95,800 complications; 18,330 HRRP.

**Tradeoffs.** CMS pages are keyed by date, so a same-day rerun overwrites the same files.

Tied to Databricks. Older duplicate landings (such as `ast\\\_rutherford\\\_mrf`) can be
deleted.

\---

## Decision 3 — Analytical store

&#x20;Delta tables in Unity Catalog (`main.bronze`; silver and gold schema
names `TODO`).

**Why.** One platform for landing, tables, transformation, and orchestration. Unlike DuckDB there is
no single-writer lock, so no separate serving copy. `dbt-databricks` supports Unity Catalog directly.

**Alternatives.** DuckDB (writer-lock conflict, local only); Postgres (works, but needs Docker and
separate hosting for a scheduled deployment).

**Bronze layout .** One table per source file, loaded **as-is: every row, no cleaning, no
filtering** (hip/knee filtering happens in silver). Each table has `\\\_source\\\_file`, `\\\_run\\\_id`, and
`\\\_ingested\\\_at`.

* **CMS datasets (4 tables):** all values as text, from the latest run's `succeeded` pages.
* **JSON price files:** streamed into a JSON Lines staging copy, then loaded one row per item in a
single `raw\\\_json` column. Spark cannot efficiently read one very large JSON document (VUMC's was 1.87 GB before it switched to CSV).
* **CSV price files:** loaded with their own columns, all text, using Delta column mapping so names
with spaces and slashes survive. Each file's metadata (hospital name, address, license) goes to a
`<key>\\\_header` table; JSON files get one too.
* Row counts are reconciled against the source; loads are idempotent; staging copies are deleted.
* **First load (2026-10-02):** all 8 tables loaded and reconciled against their sources: the four CMS
tables, Rutherford (1,469,297 rows), Williamson (94,162), VUMC (96,417 items, 65 s including the
JSON-to-JSON Lines staging), and Specialty Surgery (16,710 items).

**Tradeoffs.**

* Free Edition: one 2X-Small SQL warehouse shared by dbt and the dashboard, a daily compute quota,
and serverless-only compute.
* Spark SQL differs from DuckDB SQL.
* Williamson's wide CSV loads as 409 columns (406 from the file plus 3 lineage columns) with Delta
column mapping on. Unity Catalog column names are limited to about 255 characters.

\---

## Decision 4 — Transform + orchestration

**Transform:** dbt with `dbt-databricks`, developed in VS Code with Git and synced to a
Databricks Git folder.

* **Silver (`silver\\\_negotiated\\\_rates`):** One row per hospital, billing code, setting,
payer, and plan. JSON `raw\\\_json` is parsed with an explicit schema; Williamson's wide columns are
unpivoted by a dbt macro; all four hospitals share one schema. **Filtered to hip and knee
replacement:** CPT 27130 and 27447, and MS-DRG 469 and 470 (the codes live in the
`procedure\\\_codes` seed, so adding a procedure is a one-line change). A dbt variable,
`procedure\\\_scope: all`, keeps every procedure instead, at a much larger size and run time. Each row carries its price file's last-updated date (`file\\\_last\\\_updated\\\_on`, read from the
file's own header). Scope comes
from the four ingested files and a CCN crosswalk seed, so no county filter is needed. Silver models
are views except this table, which is materialized because parsing the VUMC JSON and unpivoting
Williamson's columns is too slow to repeat on every query.
* **Row identity and billing class .** Each silver row carries the item's full code list
(`item\\\_codes`, including the hospital's own chargemaster code) and `modifiers`, so separate
chargemaster lines that share one CPT code are not mistaken for duplicates; rows that repeat exactly
are removed. Gold groups by **billing class** so facility and professional (surgeon) fees are never
pooled. Evidence: Williamson labels both, and for CPT 27447 its facility median is $12,569 (32 rates)
against $1,244 for professional (58 rates); pooled, the professional rows set the median. Files that
do not label billing class get a documented assumption from the crosswalk seed: Rutherford is treated
as facility (flagged `billing\\\_class\\\_assumed`; its amounts are consistent with facility rates), and
VUMC stays `unspecified` until verified.
* **Code family:** Hospitals label the same code differently: VUMC lists CPT 27130 and 27447
as HCPCS, the others as CPT. Silver adds `code\\\_family` (CPT and HCPCS together), and gold compares by
it; otherwise VUMC sat alone in its own group and could not be compared.
* **Payer categories:** A hospital's median across all payers depends on how many Medicaid and Medicare
Advantage plans it lists, because those pay far less than commercial plans (Specialty Surgery's rates
run from $584 for a TennCare plan to $21,687 for commercial), so a hospital's overall median is not a
fair price to compare. Each payer/plan is sorted into commercial, exchange, Medicare Advantage,
Medicaid, or other government by keyword rules in a seed (`payer\\\_category\\\_rules.csv`); names that match
no rule default to commercial and are flagged (`payer\\\_category\\\_is\\\_default`). On the real payer list,
41 hand-checked names all landed in the intended category; about 40% of dollar-rate lines are
commercial by default, so the rules should be reviewed if commercial results look off. The
`Amerivantage` and `Network E` (exchange) rules are assumptions, and BlueCare and Bluegrass Family
Health are mapped to Medicaid after they showed up as low-paying commercial plans. Three new gold models use the categories:
`gold\\\_price\\\_by\\\_payer\\\_category`, `gold\\\_hospital\\\_comparison\\\_by\\\_payer\\\_category` (Question 1 within
commercial plans only), and `gold\\\_tn\\\_readmission\\\_ranking` (the readmission ranking of Tennessee hospitals, lowest rate first,
with a position against the state median taken from each hospital's confidence interval, and the
project hospitals flagged with their commercial prices); `gold\\\_payer\\\_category\\\_gap` (Question 2: each category's median against the
commercial median, per hospital and service).
* **Rate types:** The main comparison uses **dollar rates only**. Percentage and algorithm
rates stay in silver but are excluded from the statistics and counted per hospital and code, and
placeholder dollar rates (under $1, such as $0.01) are excluded and counted too. Converting
percentages to dollars was rejected: the base for "% of billed charges" is the whole bill, gross
charges are sometimes missing, and carve-outs are not captured. Rutherford's dollar amounts for
percent-of-charge contracts are calculated by the hospital, so they count as dollar rates; the
`methodology` column keeps that visible.
* **Gold:** `gold\\\_price\\\_variation` (per hospital, code, setting, and billing class: median, quartiles, min, max,
spread, and counts of excluded rates; each payer/plan is first collapsed to one rate, the median of its
dollar lines without a billing modifier, so a payer listed on several lines does not outweigh one listed
once; a modifier marks a variant such as a reduced service, and Rutherford lists the same CPT code at
$6,105 and $22,577 for one plan); `gold\\\_price\\\_variation\\\_across\\\_hospitals` (spread of hospital
medians for the same code, setting, and billing class; a hospital counts only with at least five
payer/plans that have a dollar rate, set by the `min\\\_payer\\\_plans\\\_for\\\_comparison` variable); `gold\\\_price\\\_vs\\\_readmission` (price next to the CMS 30-day
hip/knee readmission rate, its confidence interval, and a reported flag).
* **Hospital dimension (`dim\\\_hospital`):** One row per hospital, keyed by CCN: the crosswalk (which
price file belongs to which hospital) joined to CMS Hospital General Information (city, county,
ownership, type), flags for whether CMS readmission and complication data exist, and the price file's
last-updated date and name as stated in the file. Gold tables reference it for hospital attributes, and
`relationships` tests point at it.
* **Automated checks:** `not\\\_null` and `relationships` to the
crosswalk on hospital CCN, `accepted\\\_values` on rate type, `unique` and `not\\\_null` on the crosswalk
and CMS tables, singular tests for negative dollar rates and for inconsistent min/median/max, and a
uniqueness check on the silver grain that only warns. Rate type and methodology are part of that grain,
because VUMC lists a payer/plan on separate dollar and algorithm lines. After the VUMC CSV switch the
warning showed 145 groups: 139 from VUMC (85 mixed dollar and algorithm lines, 78 with different dollar
amounts) and 6 from Specialty Surgery; Rutherford and Williamson had none.
**First run on Databricks:** 12 models built in 48 seconds; 34 of 35 tests passed, with one warning
(154 duplicate groups on the silver grain, `TODO`: investigate before the dashboard).

**Orchestration :** One Databricks Job, `cms\\\_hospital\\\_pricing\\\_pipeline`, with three tasks in a
chain: `ingest` (notebook) → `bronze\\\_load` (notebook) → `dbt\\\_build` (dbt task: `dbt seed`, `dbt run`, `dbt test`;
catalog `main`, schema `silver`). Settings: 2 task retries, one concurrent run, job parameter `failure\\\_test`
(default `no`), and a schedule every 6 hours (America/Chicago). A full run takes about 18–23 minutes.
Retries exist at two levels: HTTP backoff with a logged line per retry in the code, and task retries in the Job.
The dbt task works on Free Edition. The dashboard reads the gold tables after each run, so there is no separate publish task.

**Run history (evidence).** The run history shows repeated green scheduled runs (Oct 14 and Oct 15), plus the failures
that taught us something: early `dbt\\\_build` failures because the seed CSVs were ignored by Git and never reached the
workspace (fixed by force-adding the three seed files), and the deliberate failure run below.

**Deliberate failure test :** The ingest notebook reads a job parameter, `failure\\\_test` (default `no`). Run with `yes`, it points one price file at a host that does not exist and runs only the price files, so the download retries with backoff (each retry is logged), the failure is recorded in the manifest, the sanity check fails the task in about a minute, and the later tasks are skipped. Nothing in the data changes, and the next normal run recovers.

**Result.** The manual run on Oct 14 with `failure\\\_test = yes` failed in 4 min 15 s with a `RunExecutionError` at `ingest`,
after the logged retries; `bronze\\\_load` and `dbt\\\_build` did not run. The next scheduled runs, with the default `no`,
succeeded end to end, so the pipeline recovered without any manual repair of data. The run-history screenshot in the
deck shows the failure followed by green runs.

**Alternatives.** dbt + Prefect (class stack): Python-native flows, but needs a separate host to run
on a schedule.



\---

## Decision 5 — Serving

A published Databricks dashboard, "Knee and Hip replacement commercial pricing in 4
hospitals in Nashville area", on the gold tables, refreshed on the Job's schedule. Two tabs:

1. **Median Price Hip and knee.** A bar chart of the median commercial outpatient facility rate for hip and knee
replacement by hospital (St. Thomas Rutherford about $22.9K, VUMC about $21.1K, Williamson about $20–21K), a detail table
(median, 25th and 75th percentile, min, max, number of payer plans), and a caveats note.
2. **Payer Gap Knee Replacement.** The spread between payer categories within each hospital, for knee replacement only.

The caveats note on the dashboard says: dollar-amount rates only, and Specialty Surgery is excluded because it labels its
setting "both" and its rates look unusually low and are unverified.

**Why.** Same platform as gold, so no serving copy or extra service to host. Dashboards are available on Free Edition.

**Changes from the plan.** The planned price-vs-readmission scatter was dropped: three hospitals cannot support an
association (see The question). Tab 2 is knee-only and not filterable by procedure; this was a deliberate choice to keep
the view simple. The Tennessee readmission ranking is kept as context (`gold\\\_tn\\\_readmission\\\_ranking`) and may be added as a
third view.

**Alternatives.** Metabase or Streamlit through the SQL warehouse: more layout control, more setup.

\---

## Known limitations

* Prices and outcomes describe different populations and periods, so any relationship is an
association, not a causal effect.
* Few hospitals plus suppressed low-volume results make the readmission analysis exploratory.
* Teaching status, volume, and case mix may affect both measures; they are shown as context, not modeled.
* Price file quality varies: missing values, placeholder rates, and percentage or algorithm rates are
flagged in silver, not silently dropped.

