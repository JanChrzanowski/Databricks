# Databricks notebook source
# MAGIC %md
# MAGIC # Reconcile catalogs
# MAGIC Diffs `catalogs_config.json` (desired state) against a small state table this
# MAGIC script maintains (`dev.default.bundle_catalog_state`) and applies CREATE,
# MAGIC `COMMENT ON` (for changed comments), and DROP accordingly.
# MAGIC
# MAGIC Safety invariant: only resources this script previously created are ever
# MAGIC candidates for DROP — matched via the state table, never via a live listing.
# MAGIC Pre-existing objects it never created (`system`, `samples`, `workspace`,
# MAGIC `default`, `information_schema`, ...) are never touched, because they were
# MAGIC never recorded in the state table to begin with.
# MAGIC
# MAGIC DROP uses RESTRICT (fails loudly if the catalog/schema is non-empty) instead
# MAGIC of CASCADE, so real data blocks the run instead of silently disappearing.

# COMMAND ----------

import json
import os

config_path = os.path.join(os.getcwd(), "catalogs_config.json")
with open(config_path) as f:
    desired = json.load(f)

STATE_TABLE = "dev.default.bundle_catalog_state"


def esc(value: str) -> str:
    return value.replace("'", "''")


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
        resource_type STRING,
        catalog_name STRING,
        schema_name STRING,
        comment STRING
    )
    """
)

state_rows = spark.table(STATE_TABLE).collect()
tracked_catalogs = {r.catalog_name: r.comment for r in state_rows if r.resource_type == "catalog"}
tracked_schemas = {
    (r.catalog_name, r.schema_name): r.comment for r in state_rows if r.resource_type == "schema"
}

desired_catalogs = {c["name"]: c.get("comment", "") for c in desired["catalogs"]}
desired_schemas = {
    (c["name"], s["name"]): s.get("comment", "")
    for c in desired["catalogs"]
    for s in c.get("schemas", [])
}

# COMMAND ----------

# Catalogs: create / update comment
for name, comment in desired_catalogs.items():
    if name not in tracked_catalogs:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {name} COMMENT '{esc(comment)}'")
        print(f"created catalog {name}")
    elif tracked_catalogs[name] != comment:
        spark.sql(f"COMMENT ON CATALOG {name} IS '{esc(comment)}'")
        print(f"updated comment on catalog {name}")

# Schemas: create / update comment
for (catalog_name, schema_name), comment in desired_schemas.items():
    full_name = f"{catalog_name}.{schema_name}"
    if (catalog_name, schema_name) not in tracked_schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_name} COMMENT '{esc(comment)}'")
        print(f"created schema {full_name}")
    elif tracked_schemas[(catalog_name, schema_name)] != comment:
        spark.sql(f"COMMENT ON SCHEMA {full_name} IS '{esc(comment)}'")
        print(f"updated comment on schema {full_name}")

# Schemas: drop (only if this script tracked it before, and it's gone from config)
for catalog_name, schema_name in tracked_schemas:
    if (catalog_name, schema_name) not in desired_schemas:
        full_name = f"{catalog_name}.{schema_name}"
        spark.sql(f"DROP SCHEMA IF EXISTS {full_name} RESTRICT")
        print(f"dropped schema {full_name}")

# Catalogs: drop (only if this script tracked it before, and it's gone from config)
for name in tracked_catalogs:
    if name not in desired_catalogs:
        spark.sql(f"DROP CATALOG IF EXISTS {name} RESTRICT")
        print(f"dropped catalog {name}")

# COMMAND ----------

# Persist the new state to diff against on the next run
rows = [("catalog", name, None, comment) for name, comment in desired_catalogs.items()]
rows += [
    ("schema", catalog_name, schema_name, comment)
    for (catalog_name, schema_name), comment in desired_schemas.items()
]

state_schema = "resource_type STRING, catalog_name STRING, schema_name STRING, comment STRING"
spark.createDataFrame(rows, state_schema).write.mode("overwrite").saveAsTable(STATE_TABLE)

print("state saved")
