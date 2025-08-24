# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Silver Layer Transformation

# COMMAND ----------

# Reading our master data that is netflix_titles
df = spark.read.format("delta")\
        .option("header", True)\
        .option("inferSchema", True)\
        .load("abfss://bronze@sonalnetflixprojectdl.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn("duration_minutes", expr("try_cast(duration_minutes AS INT)"))\
       .withColumn("duration_seasons", expr("try_cast(duration_seasons AS INT)"))
       
display(df)

# COMMAND ----------

# Now first we will get rid of null values here we can use withColumn() or fillna()
# df = fillna(0, subset = ["column names"]) but in order to fill in for multiple columns at once we use it as,

df = df.fillna({"duration_minutes": 0, "duration_seasons": 1})

# COMMAND ----------

df.display()

# COMMAND ----------

# Now we will change the datatype of the columns which our currently strings into integer

df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
         .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Now to get value before colon for the titles
df = df.withColumn("short_title", split(col("title"), ':')[0])
df.display()

# COMMAND ----------

# Now to get value before - for the titles
df = df.withColumn("short_rating", split(col("rating"), '-')[0])
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Now we will change the datatype of the columns which our currently strings into integer

# df = df.withColumn("duration_minutes", col("duration_minutes").cast(IntegerType()))\
#          .withColumn("duration_seasons", col("duration_seasons").cast(IntegerType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn(
    "f_type",
    when(col("type") == "Movie", 1)\
    .when(col("type") == "TV Show", 2)\
    .otherwise(0)
)


# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df = df.withColumn("rank_duration_minutes", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Performing aggregations

# COMMAND ----------

df1 = df.groupBy("type").agg(count("*").alias("title_count"))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC Writing the data into our silver layer

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://silver@sonalnetflixprojectdl.dfs.core.windows.net/netflix_titles")\
        .save()

# COMMAND ----------

