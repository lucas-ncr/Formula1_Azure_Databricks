# Databricks notebook source
# MAGIC %md
# MAGIC # Create a new dataframe treated to presentation standards

# COMMAND ----------

# MAGIC %md
# MAGIC | Column Name | Source |
# MAGIC |------------|----------|
# MAGIC |race_year | races|
# MAGIC |race_name | races|
# MAGIC |race_date | races|
# MAGIC |circuit_location | circuits|
# MAGIC |driver_name | drivers|
# MAGIC |driver_number | drivers|
# MAGIC |driver_nationality | drivers|
# MAGIC |team | constructors|
# MAGIC |grid | results|
# MAGIC |fastest lap | results|
# MAGIC |race time | results|
# MAGIC |points | results|
# MAGIC |created_date | current_timestamp|
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Initialization

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

circuits_df = spark.read.format("delta").load(processed_folder_path+"/circuits") \
.drop("file_date")

# COMMAND ----------

races_df = spark.read.format("delta").load(processed_folder_path+"/races").drop("file_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(processed_folder_path+"/results") \
.withColumnRenamed("file_date", "result_file_date") \
.filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(processed_folder_path+"/constructors") \
.drop("file_date")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(processed_folder_path+"/drivers") \
.drop("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Merges to obtain final_df, while dropping unnecessary columns and renaming for clarity

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, circuits_df.circuit_id, races_df.race_year, races_df.name.alias("race_name"), races_df.race_timestamp.alias("race_date"), circuits_df.location.alias("circuit_location")) \
.drop("file_date")

# COMMAND ----------

drivers_results_df = drivers_df.join(results_df, drivers_df.driver_id == results_df.driver_id, "inner") \
.select(results_df.race_id.alias("race_id_results"), results_df.constructor_id, drivers_df.name.alias("driver_name"), drivers_df.number.alias("driver_number"), 
        drivers_df.nationality.alias("driver_nationality"), results_df.grid, results_df.fastest_lap, results_df.time.alias("race_time"),
        results_df.points, results_df.position, results_df.result_file_date.alias("file_date"))

# COMMAND ----------

constructors_drivers_results_df = constructors_df.join(drivers_results_df, constructors_df.constructor_id == drivers_results_df.constructor_id, "inner")
constructors_drivers_results_df = constructors_drivers_results_df \
.drop("nationality", "constructor_id", "constructor_ref") \
.withColumnRenamed("name", "team")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Cleaning final_df and adding current_timestamp for data ingestion

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_df = races_circuits_df.join(constructors_drivers_results_df, races_circuits_df.race_id == constructors_drivers_results_df.race_id_results, "inner")
final_df = final_df.drop("circuit_id", "ingestion_date").withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Writing to f1_presentation/race_results

# COMMAND ----------

write_df = rearrange_partition_column(final_df, "race_id")
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(write_df, "f1_presentation", "race_results", "race_id", presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results LIMIT 10