# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest
# MAGIC Downloads the CMS datasets and the four hospital price files into the landing Volume,
# MAGIC logs every event to `main.bronze.ingestion_manifest`, and runs the sanity checks.
# MAGIC The task **fails** if any sanity check fails, which is what makes a bad run visible in the Job.
# MAGIC
# MAGIC **Deliberate failure test:** run the Job with the job parameter `failure_test` set to `yes`.
# MAGIC One price file is then pointed at a host that does not exist, so its download retries, fails,
# MAGIC is logged, and the task fails in about a minute. Scheduled runs use `failure_test = no`.

# COMMAND ----------

# MAGIC %pip install httpx -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from dataclasses import replace

from ingestion import config
from ingestion.run_ingest import main

dbutils.widgets.text("failure_test", "no", "Deliberate failure test (yes/no)")
failure_test = dbutils.widgets.get("failure_test").strip().lower() == "yes"

# verify_checksum=False skips re-reading the 1.9 GB VUMC file on every scheduled run.
if failure_test:
    print("DELIBERATE FAILURE TEST: pointing the first price file at a host that does not exist")
    broken = [replace(config.HOSPITAL_FILES[0], url="https://does-not-exist.invalid/standard-charges.csv")]
    broken += list(config.HOSPITAL_FILES[1:])
    run_id = main(spark, datasets=[], hospital_files=broken, verify_checksum=False)
else:
    run_id = main(spark, verify_checksum=False)

print(f"Ingestion finished. run_id = {run_id}")
