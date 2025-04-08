# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Import CSV files into a DataFrame

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

lap_times_schema = StructType(fields=[StructField('raceId', IntegerType(), False), 
                                     StructField('driverId', IntegerType(), True),
                                     StructField('lap', IntegerType(), True),
                                     StructField('position', IntegerType(), True),
                                     StructField('time', StringType(), True),
                                     StructField('milliseconds', IntegerType(), True)
                                    ])

# COMMAND ----------

# Infer schema creates a second job due to identifying and applying schema to the dataframe, inneficient in big data scenarios
lap_times_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv",  
                             schema = lap_times_schema,
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Rename columns and add ingestion_time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write data to Data Lake

# COMMAND ----------

write_df = rearrange_partition_column(lap_times_final_df, "race_id")
merge_condition = "tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id"
merge_delta_data(write_df, "f1_processed", "lap_times", "race_id", processed_folder_path, merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")