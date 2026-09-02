# Databricks notebook source
# MAGIC %md
# MAGIC # Hello World
# MAGIC Testowy notebook zarządzany przez Databricks Asset Bundle.

# COMMAND ----------

print("Hello from a bundle-managed notebook!")

# COMMAND ----------

df = spark.range(5)
display(df)
