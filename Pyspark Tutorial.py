# Databricks notebook source
# MAGIC %md
# MAGIC ## Welcome to Pyspark Tutorial
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Data Reading

# COMMAND ----------

from pyspark.sql import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PySpark-Example") \
    .getOrCreate()

df= spark.read.format("csv")\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .Load("abfss:pyspark@dlsonal.core.windows.net/BigMart Sales")

# COMMAND ----------

df.display()