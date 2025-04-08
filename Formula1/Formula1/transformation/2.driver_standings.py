# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

# COMMAND ----------

race_results_list = spark.sql(f"SELECT DISTINCT race_year FROM f1_presentation.race_results WHERE file_date = '{v_file_date}'").collect()

# COMMAND ----------

# race_results_list = spark.read.parquet(f"{presentation_folder_path}/race_results") \
# .filter(f"file_date = '{v_file_date}'") \
# .select("race_year") \
# .distinct() \
# .collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col

# race_results_df = spark.sql(f"SELECT * FROM f1_presentation.race_results WHERE race_year IN {race_year_list}")

race_results_df = spark.read.format("delta").load("/mnt/formula1dl72/presentation/race_results") \
.where(col('race_year').isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points").alias("total_points"),
     count(when(race_results_df.position == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.functions import desc, rank, asc
from pyspark.sql.window import Window

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

write_df = rearrange_partition_column(final_df, "race_year")
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(write_df, "f1_presentation", "driver_standings", "race_year", presentation_folder_path, merge_condition)