# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Ingest
# MAGIC Downloads the CMS datasets and the four hospital price files into the landing Volume,
# MAGIC logs every event to `main.bronze.ingestion_manifest`, and runs the sanity checks.
# MAGIC The task **fails** if any sanity check fails, which is what makes a bad run visible in the Job.

# COMMAND ----------

# MAGIC %pip install httpx -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from ingestion.run_ingest import main

# verify_checksum=False skips re-reading the 1.9 GB VUMC file on every scheduled run.
# Use the default (True) when running by hand if you want the full check.
run_id = main(spark, verify_checksum=False)
print(f"Ingestion finished. run_id = {run_id}")
