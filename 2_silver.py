# Databricks notebook source
# MAGIC %md
# MAGIC Silver Notebook Lookup Tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("sourcefolder","netflix_directors")
dbutils.widgets.text("targetfolder", "netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("sourcefolder")
var_trg_folder = dbutils.widgets.get("targetfolder")

# COMMAND ----------

df = spark.read.format("csv")\
         .option("Header", "true")\
         .option("inferschema", "true")\
         .load(f"abfss://bronze@sonalnetflixprojectdl.dfs.core.windows.net/{var_src_folder}")
        #  .load("abfss://bronze@sonalnetflixprojectdl.dfs.core.windows.net/netflix_directors")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .option("path", f"abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/{var_trg_folder}")\
        .save()

# COMMAND ----------

