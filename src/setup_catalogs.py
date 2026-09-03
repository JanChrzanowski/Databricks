# Databricks notebook source
# MAGIC %md
# MAGIC # Setup catalogs
# MAGIC Idempotent setup script for organization-wide Unity Catalog catalogs.
# MAGIC Safe to run repeatedly — `IF NOT EXISTS` means it only creates what's missing.
# MAGIC
# MAGIC Add schema creation here later, e.g.:
# MAGIC `spark.sql("CREATE SCHEMA IF NOT EXISTS dev.my_schema")`
# MAGIC
# MAGIC This is intentionally imperative, not the bundle's native `resources.catalogs`
# MAGIC type: that type requires an explicit `storage_root`, and these catalogs rely on
# MAGIC workspace default storage instead. Removing/renaming a catalog is a manual edit
# MAGIC here (e.g. `spark.sql("DROP CATALOG IF EXISTS old_name CASCADE")`), not an
# MAGIC automatic diff/delete on `bundle deploy`.

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS dev COMMENT 'Development environment catalog'")
spark.sql("CREATE CATALOG IF NOT EXISTS prod COMMENT 'Production environment catalog'")
