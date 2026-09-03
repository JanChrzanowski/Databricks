# Databricks notebook source
# MAGIC %md
# MAGIC # Setup catalogs
# MAGIC Data-driven, idempotent setup for organization-wide Unity Catalog catalogs
# MAGIC and schemas, driven by `catalogs_config.json` in this same folder. To add or
# MAGIC change a catalog/schema, edit that file — this script doesn't need to change.
# MAGIC
# MAGIC Still imperative: removing an entry from the config does **not** drop it in
# MAGIC Databricks. To retire a catalog/schema, drop it explicitly first (e.g.
# MAGIC `spark.sql("DROP CATALOG IF EXISTS old_name CASCADE")`) and then remove it
# MAGIC from the config.

# COMMAND ----------

import json
import os

config_path = os.path.join(os.getcwd(), "catalogs_config.json")
with open(config_path) as f:
    config = json.load(f)

# COMMAND ----------

for catalog in config["catalogs"]:
    catalog_name = catalog["name"]
    catalog_comment = catalog.get("comment", "").replace("'", "''")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name} COMMENT '{catalog_comment}'")

    for schema in catalog.get("schemas", []):
        schema_name = schema["name"]
        schema_comment = schema.get("comment", "").replace("'", "''")
        spark.sql(
            f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name} COMMENT '{schema_comment}'"
        )
