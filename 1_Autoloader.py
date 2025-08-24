# Databricks notebook source
# MAGIC %md
# MAGIC Incremental Data Loading using Autoloader

# COMMAND ----------

# Creating Schema for our Project
%sql
CREATE SCHEMA netflix_catalog.net_schema;


# COMMAND ----------

checkpoint_path = "abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_path)\
  .load("abfss://raw@sonalnetflixprojectdl.dfs.core.windows.net")

# COMMAND ----------

df.writeStream\
  .option("checkpointLocation", checkpoint_path)\
  .trigger(availableNow=True)\
  .start("abfss://bronze@sonalnetflixprojectdl.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

