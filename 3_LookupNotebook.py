# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ## Array Parameter

# COMMAND ----------

files = [
	{
		"sourcefolder": "netflix_directors",
		"targetfolder": "netflix_directors"
	},
	{
		"sourcefolder": "netflix_countries",
		"targetfolder": "netflix_countries"
	},
	{
		"sourcefolder": "netflix_category",
		"targetfolder": "netflix_category"
	},
	{
		"sourcefolder": "netflix_cast",
		"targetfolder": "netflix_cast"
	},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding array to my jobs

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "my_arr", value = files)

# COMMAND ----------

