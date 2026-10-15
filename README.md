# Nashville hip and knee replacement prices: a data pipeline

An end-to-end pipeline on Databricks that downloads hospital price transparency files and CMS quality
data, loads them into Delta tables, models them with dbt, and serves a dashboard.

**What it answers**

*By the end of this, I'll be able to answer how much the negotiated price for hip and knee replacement
varies across Nashville-area hospitals, within the same payer category, and how much it varies between
payer categories within a hospital, about four Nashville-area hospitals*

Every design choice, with the alternatives considered, is in [DECISIONS.md](DECISIONS.md).

## How it works

```
CMS Provider Data Catalog (4 datasets) ─┐
                                        ├─> landing Volume ─> bronze Delta tables ─> dbt silver ─> dbt gold ─> dashboard
Hospital price files (4 hospitals) ─────┘     (raw files)      (as-is, one per file)   (standardized)  (answers)

                 A scheduled Databricks Job runs:  ingest  ->  bronze\\\_load  ->  dbt\\\_build
```

|Layer|Where|What it holds|
|-|-|-|
|Landing|Volume `main.bronze.raw\\\_landing`|Source files exactly as downloaded|
|Manifest|Table `main.bronze.ingestion\\\_manifest`|A log of every download and page, with sizes and checksums|
|Bronze|Schema `main.bronze`|One Delta table per source file, loaded as-is|
|Silver|Schema `main.silver`|One standardized table of negotiated rates, plus staging views and seeds|
|Gold|Schema `main.gold`|Tables that answer the questions|

**The four hospitals** (all dated Jan to Oct 2026): Vanderbilt University Medical Center, Saint Thomas
Hospital for Specialty Surgery, Ascension Saint Thomas Rutherford, and Williamson Medical Center.

## What you need

* A **Databricks workspace**. The free [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
is enough (one 2X-Small SQL warehouse and serverless compute).
* **Free Edition limits internet access** to a short list of sites. Verify your account (the
"Verify identity" button in the top bar, through LinkedIn), which unlocks the outbound access this
pipeline needs to download the data.
* A **GitHub account** and **Git**.
* **Python 3.10 to 3.12** on your own computer, to run dbt locally (optional if you only use the Job).

Expect about 20 minutes for the first full run. Nothing is installed on your computer except dbt, and
no Docker is needed.

## Run it from zero

### 1\. Get the code into Databricks

1. Fork or clone this repository on GitHub.
2. In Databricks, go to **Workspace**, then **Create**, then **Git folder**. Paste your repository's URL
and create the folder.

### 2\. Create the catalog objects

Open `notebooks/00\\\_setup\\\_catalog.sql` in the Git folder, attach **Serverless** compute, and run its
first cells. They create the schema `main.bronze`, the landing Volume `main.bronze.raw\\\_landing`, and
the manifest table.

> \\\*\\\*If you cannot use the catalog name `main`:\\\*\\\* pick another name and change it in three places:
> `CATALOG` in `ingestion/config.py`, `database: main` in `dbt\\\_project/models/bronze/sources.yml`, and
> `catalog` in your dbt profile (step 5). Then replace `main` in the SQL queries in this README.

### 3\. Check that downloads work

In a notebook cell, run the connectivity check. It tests the CMS API and each hospital file, and prints
one verdict per URL:

```python
from ingestion.check\\\_internet\\\_access import main
main()
```

Every line should say `OK`. If one says `BLOCKED?`, see Troubleshooting.

### 4\. Run ingestion and the bronze load by hand

1. Open `notebooks/01\\\_ingest` and click **Run all**. It downloads the CMS datasets and the price files
(about 14 minutes the first time, mostly the CMS API) and prints a sanity report for each source.
The task fails if any check fails.
2. Open `notebooks/02\\\_bronze\\\_load` and click **Run all** (about 2 minutes). It builds the bronze tables
and checks each table's row count against its source.

Both notebooks install their own packages (`httpx`, `ijson`) at the top.

Check the result:

```sql
SELECT 'hospital\\\_general\\\_information' AS t, COUNT(\\\*) AS n FROM main.bronze.hospital\\\_general\\\_information
UNION ALL SELECT 'unplanned\\\_hospital\\\_visits', COUNT(\\\*) FROM main.bronze.unplanned\\\_hospital\\\_visits
UNION ALL SELECT 'complications\\\_and\\\_deaths', COUNT(\\\*) FROM main.bronze.complications\\\_and\\\_deaths
UNION ALL SELECT 'struth\\\_mrf', COUNT(\\\*) FROM main.bronze.struth\\\_mrf
UNION ALL SELECT 'wmc\\\_mrf', COUNT(\\\*) FROM main.bronze.wmc\\\_mrf
UNION ALL SELECT 'sthss\\\_mrf', COUNT(\\\*) FROM main.bronze.sthss\\\_mrf
```

At the time of writing (October 2026) these were about 5,419, 67,060, 95,800, 1,469,297, 94,162 and
16,710. CMS refreshes its data every quarter, so expect different numbers over time.

### 5\. Run dbt from your computer

```bash
git clone <your-repository-url>
cd <repository>
python -m venv .venv
source .venv/bin/activate            # Windows PowerShell: .venv\\\\Scripts\\\\Activate.ps1
pip install -r requirements.txt
```

Create your connection profile. It lives **outside** the repository, so no secret is ever committed:

```bash
mkdir -p \\\~/.dbt                      # Windows: New-Item -ItemType Directory -Force $HOME\\\\.dbt
cp dbt\\\_project/profiles.yml.example \\\~/.dbt/profiles.yml     # Windows: Copy-Item ... $HOME\\\\.dbt\\\\profiles.yml
```

Set three environment variables (they last for that terminal session):

|Variable|Where to find it|
|-|-|
|`DATABRICKS\\\_HOST`|Your workspace address, without `https://` (for example `dbc-xxxx.cloud.databricks.com`)|
|`DATABRICKS\\\_HTTP\\\_PATH`|**SQL Warehouses**, your warehouse, **Connection details** (looks like `/sql/1.0/warehouses/<id>`)|
|`DATABRICKS\\\_TOKEN`|**Settings**, **Developer**, **Access tokens**, **Generate new token**. Never commit it.|

```bash
export DATABRICKS\\\_HOST="..."         # Windows PowerShell: $env:DATABRICKS\\\_HOST = "..."
export DATABRICKS\\\_HTTP\\\_PATH="..."
export DATABRICKS\\\_TOKEN="..."
```

Then build and test the models:

```bash
cd dbt\\\_project
dbt debug        # "All checks passed!"
dbt seed         # loads the three small lookup tables
dbt run          # builds silver and gold (about a minute)
dbt test         # runs the data quality tests
```

You should see all models build and the tests finish with **no errors** and **one warning**: a
documented check for repeated payer lines in silver (see DECISIONS.md). `dbt docs generate` and
`dbt docs serve` show the lineage graph.

### 6\. Put it on a schedule (Databricks Job)

In **Jobs \& Pipelines**, create a job named `cms\\\_hospital\\\_pricing\\\_pipeline` with three tasks:

|Task|Type and settings|Depends on|
|-|-|-|
|`ingest`|Notebook, Workspace source, path `notebooks/01\\\_ingest` in your Git folder, Serverless|none|
|`bronze\\\_load`|Notebook, path `notebooks/02\\\_bronze\\\_load`, Serverless|`ingest`|
|`dbt\\\_build`|**dbt** task, Workspace source, project directory `dbt\\\_project`, your SQL warehouse, **Catalog `main`**, **Schema `silver`**, commands `dbt seed`, `dbt run`, `dbt test` (no `dbt deps`), a 1.12 release of `dbt-databricks`|`bronze\\\_load`|

Then set:

* **Retries:** 2 retries, 30 seconds apart, on every task.
* **Job parameter:** `failure\\\_test` = `no` (see "Failure test" below).
* **Maximum concurrent runs:** 1.
* **Schedule:** a scheduled trigger, for example every 6 hours in your time zone.

The job runs whatever code is in the Git folder, so **push, then Pull in Databricks** after any change.
The price files are skipped when their servers say they have not changed, and everything else is
safe to repeat.

### 7\. Build the dashboard

In **Dashboards**, create three datasets from SQL and chart them:

```sql
-- commercial\\\_prices: commercial facility rates by hospital (bar chart plus table)
SELECT hospital\\\_name, procedure\\\_label, median\\\_rate, p25\\\_rate, p75\\\_rate,
       min\\\_rate, max\\\_rate, n\\\_payer\\\_plans, file\\\_last\\\_updated\\\_on
FROM main.gold.gold\\\_price\\\_by\\\_payer\\\_category
WHERE payer\\\_category = 'commercial' AND billing\\\_class = 'facility'
  AND setting = 'outpatient' AND code IN ('27130', '27447') AND n\\\_payer\\\_plans >= 3
ORDER BY procedure\\\_label, median\\\_rate;

-- payer\\\_gap\\\_knee: knee replacement rate by payer category (bar chart)
SELECT hospital\\\_name, payer\\\_category, median\\\_rate, n\\\_payer\\\_plans
FROM main.gold.gold\\\_price\\\_by\\\_payer\\\_category
WHERE code = '27447' AND billing\\\_class = 'facility' AND setting = 'outpatient'
  AND median\\\_rate IS NOT NULL
ORDER BY hospital\\\_name, payer\\\_category;

-- tn\\\_readmission\\\_ranking: Tennessee hospitals ranked on readmissions (bar chart, optional)
SELECT rank\\\_lowest\\\_first, cms\\\_hospital\\\_name, readmission\\\_rate, readmission\\\_lower, readmission\\\_upper,
       comparison\\\_to\\\_state, is\\\_project\\\_hospital, commercial\\\_knee\\\_median
FROM main.gold.gold\\\_tn\\\_readmission\\\_ranking
ORDER BY rank\\\_lowest\\\_first;
```

State these caveats on the dashboard: dollar rates only (percentage and algorithm rates are excluded),
facility fees only, outpatient setting, and payer categories assigned by keyword rules.

## Failure test

To see the failure handling, start the job with **Run now with different parameters** and set
`failure\\\_test` to `yes`. One price file is pointed at a host that does not exist, so its download
retries with backoff (each retry is logged), the failure is written to the manifest, the sanity check
fails the `ingest` task, and the later tasks are skipped. No data changes, and the next normal run
recovers. Do not use **Repair run** on it, which repeats with `yes`.

## Start over from zero

To test that the instructions work from a clean state, drop everything and rerun step 2 onward. Dropping
`main.bronze` also removes the landing Volume and all downloaded files:

```sql
DROP SCHEMA IF EXISTS main.gold   CASCADE;
DROP SCHEMA IF EXISTS main.silver CASCADE;
DROP SCHEMA IF EXISTS main.bronze CASCADE;
```

## Repository layout

```
ingestion/          Python package: API clients, downloader, manifest, sanity checks, bronze load
notebooks/          00\\\_setup\\\_catalog (one-time setup), 01\\\_ingest and 02\\\_bronze\\\_load (the Job's notebooks)
dbt\\\_project/        dbt project
  seeds/            hospital crosswalk (CCNs), procedure codes, payer category rules
  macros/           shared SQL: parsing the price file layouts
  models/bronze/    declarations of the bronze tables (sources)
  models/silver/    staging views and silver\\\_negotiated\\\_rates
  models/gold/      dim\\\_hospital and the gold tables
  tests/            custom data quality tests
DECISIONS.md        design choices, alternatives, tradeoffs
requirements.txt    dbt dependency
```

## Data sources

* **CMS Provider Data Catalog** (data.cms.gov/provider-data): Hospital General Information
(`xubh-q36u`), Unplanned Hospital Visits (`632h-zaca`), Complications and Deaths (`ynj2-r877`) and the
Hospital Readmissions Reduction Program file (`9n3s-kdb3`). These are public CMS data, refreshed
quarterly.
* **Hospital price transparency files:** machine-readable files that each hospital must publish. The
links are in `HOSPITAL\\\_FILES` in `ingestion/config.py`. They come in three layouts (a nested JSON
file, two "tall" CSVs, and a "wide" CSV), and the pipeline handles each.

## Quality checks

* **At ingestion:** retry with backoff, one source failing never stops the others, and a sanity check
per source compares the manifest, the files on disk, and the expected size or row count.
* **At bronze:** every table's row count is reconciled against its source file.
* **In dbt:** not-null, unique, relationship and accepted-value tests, custom tests for negative rates
and inconsistent statistics, and one deliberate warning on repeated payer lines.

## Troubleshooting

|Symptom|Likely cause and fix|
|-|-|
|A URL shows `BLOCKED?` in the connectivity check|Free Edition limits internet access. Verify your identity in the top bar and rerun.|
|Ingest fails with "expected JSON/CSV but payload..." for a hospital|The hospital moved or withdrew the file (this happened once). Find the new link on its price transparency page, then update `url` and `file\\\_format` in `ingestion/config.py`.|
|`dbt` is not recognized|Your virtual environment is not active. Activate it, then run `dbt --version`.|
|`dbt debug` cannot connect|Check the three environment variables, and that the SQL warehouse is running (it stops after 10 idle minutes).|
|The Job's dbt task fails with "depends on a node named ..."|The seed CSVs did not reach Git. Make sure `dbt\\\_project/seeds/\\\*.csv` are committed (a `.gitignore` rule for `\\\*.csv` hides them).|
|dbt tables appear in the wrong catalog|Set **Catalog** on the Job's dbt task, and `catalog` in your profile.|
|`UNRESOLVED\\\_COLUMN` for a bronze column|The bronze table has an older layout. Run `02\\\_bronze\\\_load` again, then `dbt run`.|
|Runs fail with a quota message|Free Edition has a daily compute limit. Lengthen the schedule interval.|
|PowerShell will not activate the environment|Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once.|

## Limitations

* Only four hospitals, so the results describe patterns and do not support statistical claims.
* Dollar rates only: percentage and algorithm-based rates are excluded and counted.
* Payer categories come from keyword rules, so names that match no rule default to commercial.
* Specialty Surgery has no CMS readmission data, and its commercial rates are unusually low and
unverified.
* Hospital file links change over time. Update them in `ingestion/config.py` when they do.

