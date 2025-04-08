# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Import JSON file into a DataFrame

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False), 
                                     StructField('driverId', IntegerType(), True),
                                     StructField('stop', IntegerType(), True),
                                     StructField('lap', IntegerType(), True),
                                     StructField('time', StringType(), True),
                                     StructField('duration', StringType(), True),
                                     StructField('milliseconds', IntegerType(), True)
                                    ])

# COMMAND ----------

# Infer schema creates a second job due to identifying and applying schema to the dataframe, inneficient in big data scenarios
pit_stops_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json",  
                             schema = pit_stops_schema,
                             multiLine = True
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Rename columns and add ingestion_time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumn("data_source", lit(v_data_source))


# COMMAND ----------

pit_stops_final_df = add_ingestion_date(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write data to Data Lake

# COMMAND ----------

write_df = rearrange_partition_column(pit_stops_final_df, "race_id")

incremental_write(write_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")