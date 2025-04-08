# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_list = spark.sql(f"SELECT DISTINCT race_year FROM f1_presentation.race_results WHERE file_date = '{v_file_date}'").collect()

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

race_results_df = spark.read.format("delta").load("/mnt/formula1dl72/presentation/race_results") \
.where(col('race_year').isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(race_results_df.position == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.functions import desc, rank, asc
from pyspark.sql.window import Window

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

write_df = rearrange_partition_column(final_df, "race_year")
merge_condition = "tgt.total_points = src.total_points AND tgt.wins = src.wins AND tgt.rank = src.rank AND tgt.race_year = src.race_year"
merge_delta_data(write_df, "f1_presentation", "constructor_standings", "race_year", presentation_folder_path, merge_condition)