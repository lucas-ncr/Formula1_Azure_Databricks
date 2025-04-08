# Databricks notebook source
# spark.read.json("/mnt/formula1dl72/raw/2021-03-28/results.json").createOrReplaceTempView("results_cutover")

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Import JSON file into a DataFrame

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField('constructorId', IntegerType(), False), 
                                    StructField('raceId', IntegerType(), False), 
                                    StructField('driverId', IntegerType(), False),
                                    StructField('fastestLap', IntegerType(), True), 
                                    StructField('fastestLapSpeed', StringType(), True),
                                    StructField('fastestLapTime', StringType(), True),
                                    StructField('grid', IntegerType(), False),
                                    StructField('laps', IntegerType(), False),
                                    StructField('milliseconds', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('points', FloatType(), False),
                                    StructField('position', IntegerType(), True),
                                    StructField('positionOrder', IntegerType(), False),
                                    StructField('positionText', StringType(), False),
                                    StructField('rank', IntegerType(), True),
                                    StructField('resultId', IntegerType(), False),
                                    StructField('statusId', IntegerType(), False),
                                    StructField('time', StringType(), True)
                                    ])

# COMMAND ----------

# Infer schema creates a second job due to identifying and applying schema to the dataframe, inneficient in big data scenarios
results_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/results.json",  
                             schema = results_schema
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Drop unwanted columns from the dataframe

# COMMAND ----------

results_dropped_df = results_df.drop('statusId')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Rename the columns according to naming standards and add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

# COMMAND ----------

# Renaming columns according to naming standards
results_renamed_df = results_dropped_df.withColumnRenamed("resultId", "result_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("positionOrder", "position_order")\
.withColumnRenamed("positionText", "position_text")\
.withColumnRenamed("fastestLap", "fastest_lap")\
.withColumnRenamed("fastestLapTime", "fastest_lap_time")\
.withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
.withColumn("data_source", lit("v_data_source"))\
.withColumn("file_date", lit(v_file_date))
    

# COMMAND ----------

# Adding ingestion_date
results_final_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write data to Data Lake

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode("append").format("parquet").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# COMMAND ----------

results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed", "data_source", "file_date", "ingestion_date", "race_id")

# COMMAND ----------

if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    results_final_df.write.mode("overwrite").insertInto("f1_processed.results")
else:
    results_final_df.write.mode("append").format("parquet").partitionBy("race_id").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Diagnostic code

# COMMAND ----------

# %sql
# SELECT race_id, COUNT(1)
# FROM f1_processed.results
# GROUP BY race_id
# ORDER BY race_id DESC;

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results

# COMMAND ----------

# spark.sql("MSCK REPAIR TABLE f1_processed.results")