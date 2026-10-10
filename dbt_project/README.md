# dbt project: cms_hospital_pricing

Silver and gold models on top of the bronze tables in `main.bronze`.

## Run

```bash
pip install dbt-databricks
cp profiles.yml.example ~/.dbt/profiles.yml      # then set the environment variables below
export DATABRICKS_HOST=...        # workspace host, no https://
export DATABRICKS_TOKEN=...       # personal access token; never commit it
dbt debug && dbt seed && dbt run && dbt test
dbt docs generate && dbt docs serve    # lineage graph
```

## Useful switches

By default silver and gold cover only the hip and knee codes in `seeds/procedure_codes.csv`
(CPT 27130 and 27447, MS-DRG 469 and 470). To change that:

```bash
dbt run --vars '{procedure_scope: all}'           # every procedure (much larger and slower)
dbt run --vars '{procedure_scope: all, silver_code_types: all}'   # ...and hospital-local code types too
```

Add a row to `seeds/procedure_codes.csv` to include another procedure in the default scope.

Silver models are views except `silver_negotiated_rates` (a table, because parsing the VUMC JSON
and unpivoting Williamson's wide columns is too slow to repeat). Gold models are tables.

`dim_hospital` (gold) is the hospital dimension: one row per hospital with CMS attributes, outcome-data flags,
and the price file's last-updated date. Each silver rate row carries `file_last_updated_on`.
