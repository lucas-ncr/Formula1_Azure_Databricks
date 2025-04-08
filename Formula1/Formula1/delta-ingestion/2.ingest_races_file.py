# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Import CSV file into a DataFrame

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), nullable = False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), False),
                                     StructField("circuitId", IntegerType(), nullable = True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# Infer schema creates a second job due to identifying and applying schema to the dataframe, inneficient in big data scenarios
races_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/races.csv", 
                             header = True, 
                             schema = races_schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Select only the required columns

# COMMAND ----------

races_selected_df = races_df.select(races_df.raceId, races_df.year, races_df.round, races_df.circuitId, races_df.name, races_df.date, races_df.time)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Rename the columns according to naming standards

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("year", "race_year") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

# Create ingestion date parameter
races_final_df = add_ingestion_date(races_renamed_df)

# COMMAND ----------

# Group date and time to form race_timestamp
races_final_df = races_final_df.withColumn("race_timestamp", to_timestamp(concat(races_final_df.date, lit(' '), races_final_df.time), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# Remove date and time columns
races_final_df = races_final_df.drop("date", "time")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write data to Data Lake

# COMMAND ----------

races_final_df.write.partitionBy("race_year").mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")