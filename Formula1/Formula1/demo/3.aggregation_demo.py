# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use aggregate functions on race results

# COMMAND ----------

final_df = spark.read.parquet(presentation_folder_path+"/race_results")

# COMMAND ----------

demo_df = final_df.where("race_year = 2020")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("points")).show()

# COMMAND ----------

demo_df.select(countDistinct("points")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.where("driver_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)", "total_points") \
.withColumnRenamed("count(DISTINCT race_name)", "total_races").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group by method

# COMMAND ----------

grouped_by_driver_df = demo_df.groupBy("driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

display(grouped_by_driver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

demo_df = final_df.where("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(100)