# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file

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

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField('forename', StringType(), True), StructField('surname', StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField('driverId', IntegerType(), False), 
                                    StructField('driverRef', StringType(), True), 
                                    StructField('number', IntegerType(), True), 
                                    StructField('code', StringType(), True),
                                    StructField('name', name_schema, True),
                                    StructField('dob', DateType(), True),
                                    StructField('nationality', StringType(), True),
                                    StructField('url', StringType(), True)
                                    ])

# COMMAND ----------

# Infer schema creates a second job due to identifying and applying schema to the dataframe, inneficient in big data scenarios
drivers_df = spark.read.json(f"{raw_folder_path}/{v_file_date}/drivers.json",  
                             schema = drivers_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Drop unwanted columns from the dataframe

# COMMAND ----------

drivers_dropped_df = drivers_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Rename the columns according to naming standards and add ingestion_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

# COMMAND ----------

drivers_final_df = drivers_dropped_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_final_df = add_ingestion_date(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write data to Data Lake

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")