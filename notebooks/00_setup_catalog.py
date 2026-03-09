# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Setup Unity Catalog resources for BD TOPO

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("schema", "ign_bdtopo")
dbutils.widgets.text("volume", "bronze_volume")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"Schema {catalog}.{schema} ready.")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
print(f"Volume {catalog}.{schema}.{volume} ready.")
