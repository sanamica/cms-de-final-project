# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 Bronze load
# MAGIC Loads every landed file into its bronze Delta table in `main.bronze`, as-is, and checks each
# MAGIC table's row count against the source. A table already built from the latest run is skipped,
# MAGIC so this is safe to run on every schedule. The task **fails** if any table fails.

# COMMAND ----------

# MAGIC %pip install ijson -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from ingestion.bronze_load import load_all

results = load_all(spark)
print(results)