# Databricks notebook source
# MAGIC %md
# MAGIC # Setup catalogs
# MAGIC Idempotent setup script for organization-wide Unity Catalog catalogs.
# MAGIC Safe to run repeatedly — `IF NOT EXISTS` means it only creates what's missing.
# MAGIC
# MAGIC Add schema creation here later, e.g.:
# MAGIC `spark.sql("CREATE SCHEMA IF NOT EXISTS dev.my_schema")`

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dev COMMENT 'Development environment catalog'")
spark.sql("CREATE CATALOG IF NOT EXISTS prod COMMENT 'Production environment catalog'")
