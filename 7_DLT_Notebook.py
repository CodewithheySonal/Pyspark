# Databricks notebook source
# MAGIC %md
# MAGIC # DLT- NB Gold Layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC follwing code is our Data quality check for DLT

# COMMAND ----------

Looktables_rules = {
    "rule1" : "show_id is NOT NULL"
}

# COMMAND ----------

# @dlt.table(
#     name="your_table_name",
#     comment="Description of the table",
#     partition_cols=["partition_column"],
#     table_properties={"delta.autoOptimize.optimizeWrite": "true"}
# )
# def your_table_name():
#     # Replace with your DataFrame logic
#     return spark.read.format("csv").option("header", "true").load("/path/to/your/data.csv")

# COMMAND ----------

# The @dlt.expect_all_or_drop(Looktables_rules) decorator in Delta Live Tables (DLT) is a powerful way to implement data quality checks and define a specific behavior when those checks fail: dropping the offending rows. Like- warn, drop and fail.
#  @dlt.expect_all_or_drop(Looktables_rules) means that your DLT table will only include rows that satisfy every data quality rule defined in Looktables_rules; any row that fails even one of these rules will be silently dropped from the output table.

# COMMAND ----------

@dlt.table(
    name = "gold_netflixdirectors"
)

@dlt.expect_all_or_drop(Looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load(abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/netflix_directors)
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcategory"
)

@dlt.expect_all_or_drop(Looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load(abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/netflix_category)
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcast"
)

@dlt.expect_all_or_drop(Looktables_rules)
def myfunc():
    df = spark.readStream.format("delta").load(abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/netflix_cast)
    return df

# COMMAND ----------

@dlt.table(
    name = "gold_netflixcountries"
)

# @dlt.expect_all_or_drop(Looktables_rules) we can also write it as
@dlt.expect_or_drop( "rule1", "show_id is NOT NULL")
def myfunc():
    df = spark.readStream.format("delta").load(abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/netflix_countries)
    return df

# COMMAND ----------

# Now doing it for another table in different way

@dlt.table

def gold_stg_netflixtitles():
    df = spark.readStream.format("delta").load(abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/netflix_titles)
    return df

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

@dlt.view

def gold_trans_netflixtitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("new_flag", lit(1)) #in case you forget to make any transformation you can do it right here.
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC Now creating our final table

# COMMAND ----------

master_rules ={
    "rule1" : "new_flag is NOT NULL"
    "rule2" : "show_id is NOT NULL"
}

# COMMAND ----------

@dlt.table

@dlt.expect_all_or_drop(master_rules)
def gold_netflictitles():
    df = spark.readStream.table("LIVE.gold_trans_netflixtitles")
    return df