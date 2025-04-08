# Databricks notebook source
# MAGIC %md
# MAGIC # Understand how to perform filter (SQL WHERE clause) on Dataframes

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(processed_folder_path+"/races")

# COMMAND ----------

# Using SQL language method
races_filtered_df = races_df.where("race_year = 2019 AND round <= 5")

# COMMAND ----------

# Using Python language method
races_filtered_df = races_df.where((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

races_filtered_df.count()