# Databricks notebook source
# MAGIC %md
# MAGIC # Perform join transformations on dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner join

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(processed_folder_path+"/circuits") \
.where("circuit_id < 70") \
.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(processed_folder_path+"/races") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

races_filtered_df = races_df.where("race_year = 2019")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left outer join

# COMMAND ----------

# Left Outer Join
race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right outer join

# COMMAND ----------

# Right outer join
race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full outer join

# COMMAND ----------

# Full outer join
race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_df.circuit_id, "outer") \
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi joins
# MAGIC Everything in the inner join, only in the left dataframe (excludes right dataframe)

# COMMAND ----------

# Semi join
race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti joins
# MAGIC Opposite of semi join, everything on left dataframe that is excluded from right dataframe

# COMMAND ----------

# Anti join
race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross joins
# MAGIC The cartesian product of dataframe, extremely large result if both tables are big \
# MAGIC Should be use when wanting to join all columns from left dataframe to right dataframe

# COMMAND ----------

# Cross join
race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_df.circuit_id, "cross")