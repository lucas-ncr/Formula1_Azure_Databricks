# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying folder

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

qualifying_schema = StructType(fields=[
                                     StructField('qualifyId', IntegerType(), False), 
                                     StructField('raceId', IntegerType(), False),
                                     StructField('driverId', IntegerType(), False),
                                     StructField('constructorId', IntegerType(), False),
                                     StructField('number', IntegerType(), True),
                                     StructField('position', IntegerType(), True),
                                     StructField('q1', StringType(), True),
                                     StructField('q2', StringType(), True),
                                     StructField('q3', StringType(), True)
                                    ])

# COMMAND ----------

# Infer schema creates a second job due to identifying and applying schema to the dataframe, inneficient in big data scenarios
qualifying_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/qualifying/qualifying_split*.json",  
                             schema = qualifying_schema,
                             multiLine = True
                             )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Rename columns and add ingestion_time

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("driverId", "driver_id")\
.withColumnRenamed("raceId", "race_id")\
.withColumnRenamed("constructorId", "constructor_id")\
.withColumnRenamed("qualifyId", "qualifying_id")\
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write data to Data Lake

# COMMAND ----------

write_df = rearrange_partition_column(qualifying_final_df, "race_id")

incremental_write(write_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

dbutils.notebook.exit("Success")