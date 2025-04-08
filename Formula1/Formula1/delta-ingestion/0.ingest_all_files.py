# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-03-21"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source" : "Ergast API", "p_file_date" : "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop duplicates

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE
# MAGIC FROM f1_processed.results
# MAGIC WHERE driver_id IN (
# MAGIC     SELECT driver_id
# MAGIC     FROM f1_processed.results
# MAGIC     GROUP BY race_id, driver_id
# MAGIC     HAVING (COUNT(race_id) > 1 AND COUNT(driver_id) > 1)
# MAGIC )
# MAGIC AND race_id IN (
# MAGIC     SELECT race_id
# MAGIC     FROM f1_processed.results
# MAGIC     GROUP BY race_id, driver_id
# MAGIC     HAVING (COUNT(race_id) > 1 AND COUNT(driver_id) > 1)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1