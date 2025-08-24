# Databricks notebook source
# MAGIC %md
# MAGIC Reading any other notebook inside this notebook- In short notebook inside another notebook we. use following commands like % run

# COMMAND ----------

# MAGIC %run /Workspace/Databrickszero2hero/Tutorial

# COMMAND ----------

# MAGIC %md
# MAGIC # DELTA LAKE

# COMMAND ----------

df.write.format('delta')\
        .mode("append")\
        .option('path', 'abfss://destination@dlsonal.dfs.core.windows.net/sales')\
        .save()
        

# COMMAND ----------

# MAGIC %md
# MAGIC # MANAGED AND UNMANAGED DELTA TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC CREATING DATABASE FIRST which will be stored in our Metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG ws_adb_sonal;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE salesDB;

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE salesDB.mantable(
# MAGIC   ID INT,
# MAGIC   Name STRING,
# MAGIC   Marks INT
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesDB.mantable 
# MAGIC VALUES
# MAGIC (1, "aa", 10),
# MAGIC (2, "bb", 20),
# MAGIC (3, "cc", 30)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.mantable;

# COMMAND ----------

# MAGIC %md
# MAGIC You can drop the above table which will also delete the data residing inside it.

# COMMAND ----------

# MAGIC %md
# MAGIC **External Table**

# COMMAND ----------

# MAGIC %md
# MAGIC Before creating your external table you must have's:
# MAGIC 1. Role assignment created at storage level for Storage Blob Data Contributor.
# MAGIC 2. Credential created at catalog level to access the ADLS data using Access connector for DB using it's Resource ID.
# MAGIC 3. Creating external table at catalog level for the same credential and granting permission to it.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE salesDB.exttable(
# MAGIC   ID INT,
# MAGIC   Name STRING,
# MAGIC   Marks INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://destination@dlsonal.dfs.core.windows.net/salesDB';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesDB.exttable 
# MAGIC VALUES
# MAGIC (1, "aa", 10),
# MAGIC (2, "bb", 20),
# MAGIC (3, "cc", 30)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.exttable;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Functionalities

# COMMAND ----------

# MAGIC %md
# MAGIC INSERT

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesDB.exttable 
# MAGIC VALUES
# MAGIC (4, "aa", 10),
# MAGIC (5, "bb", 20),
# MAGIC (6, "cc", 30)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.exttable;

# COMMAND ----------

# MAGIC %md
# MAGIC DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from salesDB.exttable
# MAGIC where ID = 6;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.exttable;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY salesDB.exttable;

# COMMAND ----------

# MAGIC %md
# MAGIC **VESONING- TIME TRAVEL**

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE salesDB.exttable TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.exttable;

# COMMAND ----------

# MAGIC %md
# MAGIC **VACUUM- when we need to free up the files from the physical storage**

# COMMAND ----------

#don't run this command it's just mentioned for refrence. Since it will remove all unnecessary files required which will also finish time travelling.
# VACUUM salesDB.tablename
# Here the by default retention period is 7 days. It means we can vacuum any files less than of 7 days. Still we we want to delete it. then following is the RETAIN command for it.
# VACUUM tablename RETAIN 0 hours;


# COMMAND ----------

# MAGIC %md
# MAGIC **Delta Table Optimization**

# COMMAND ----------

# MAGIC %md
# MAGIC **OPTIMIZE**

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE salesDB.exttable;
# MAGIC -- # This will create an impact once it is used in larger tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.exttable

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE salesDB.exttable ZORDER BY (id);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.exttable

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader using Spark Streaming

# COMMAND ----------

df = spark.readStream.format(cloudFiles)\
                     .option("cloudFiles.format", parquet)\
                     .option("cloudFiles.schemaLocation", "abfss://aldestination@dlsonal.dfs.core.windows.net/checkpoint")\
                     .Load( "abfss://alsource@dlsonal.dfs.core.windows.net/")

# COMMAND ----------

df.writeStream.format("delta")\
              .option("checkpointLocation", "abfss://aldestination@dlsonal.dfs.core.windows.net/checkpoint")\
              .Trigger(processingTime = "5 seconds")\
              .start("abfss://aldestination@dlsonal.dfs.core.windows.net/data")
              