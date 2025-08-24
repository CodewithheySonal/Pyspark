# Databricks notebook source
# MAGIC %md
# MAGIC ### To Access any external data storage like- ADLS Gen2

# COMMAND ----------

# MAGIC %md
# MAGIC These values are fetch using a Service Principal in azure.

# COMMAND ----------

secret_value = "yRn8Q~~qlor_O2XUUkg-FCIRXkp9iN066FZpgafr" # will store this value later in key vault
App_id = "40af543d-4a04-4e84-8200-04dc1e12a0dd"
tenanat_id = "6fc27284-18de-41e9-8d4f-c7515e710cf5"

# COMMAND ----------

# MAGIC %md
# MAGIC this code will only run through your own cluster not serverless coz it allows your cluster to prive access to read the info

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dlsonal.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlsonal.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlsonal.dfs.core.windows.net", "40af543d-4a04-4e84-8200-04dc1e12a0dd")
spark.conf.set("fs.azure.account.oauth2.client.secret.dlsonal.dfs.core.windows.net", "yRn8Q~~qlor_O2XUUkg-FCIRXkp9iN066FZpgafr")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlsonal.dfs.core.windows.net", "https://login.microsoftonline.com/6fc27284-18de-41e9-8d4f-c7515e710cf5/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #DB Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.fs()** to read files under particular location

# COMMAND ----------

dbutils.fs.ls("abfss://source@dlsonal.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.widgets()** For creating interactive input fields in notebooks. Like parameterized inputs,etc.

# COMMAND ----------

dbutils.widgets.text("p_name", "Sonal")

# COMMAND ----------

# now if you want to provide a value to p_name other than sonal
var = dbutils.widgets.get("p_name")
print(var)

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.secrets** (Secrets Management Utilities): For secure handling of sensitive information.

# COMMAND ----------

# MAGIC %md
# MAGIC This scope gets generated using the Databricks crope credentials: {ADB WS URL}/#secrets/createScope
# MAGIC https://adb-2353204917612307.7.azuredatabricks.net/?o=2353204917612307#secrets/createScope

# COMMAND ----------

dbutils.secrets.list(scope = "sonalscope")

# COMMAND ----------

dbutils.secrets.get(scope = "sonalscope", key = "app-secret-masterclass")
# REDACTED means your value is pulled and secured.

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('csv')\
               .option("header", True)\
               .option("inferSchema", True)\
               .load("abfss://source@dlsonal.dfs.core.windows.net/")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Doing some transformation for learning purpose:

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Transforming the column Item_Type in such a manner that I want a list of all the items separated by a ','

# COMMAND ----------

df.withColumn('Item_Type', split(col('Item_Type'), ' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, What if I want to create a flag colun with the result stored in var in earch row of the table and need to create a column for it

# COMMAND ----------

df.withColumn('Flag', lit(var)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Now, whatif I want to change the datatype of any column say for Item_Visibility: Then in such cases we cast the column into other datatypes

# COMMAND ----------

df.withColumn('Item_Visibility', col('Item_visibility').cast('string')).display()

# COMMAND ----------



# COMMAND ----------

