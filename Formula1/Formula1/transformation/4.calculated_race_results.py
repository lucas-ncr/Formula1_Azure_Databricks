# Databricks notebook source
# MAGIC %md
# MAGIC # Create a new dataframe treated to presentation standards

# COMMAND ----------

# MAGIC %md
# MAGIC | Column Name | Source |
# MAGIC |------------|----------|
# MAGIC |race_year | races|
# MAGIC |race_name | races|
# MAGIC |race_date | races|
# MAGIC |circuit_location | circuits|
# MAGIC |driver_name | drivers|
# MAGIC |driver_number | drivers|
# MAGIC |driver_nationality | drivers|
# MAGIC |team | constructors|
# MAGIC |grid | results|
# MAGIC |fastest lap | results|
# MAGIC |race time | results|
# MAGIC |points | results|
# MAGIC |created_date | current_timestamp|
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results(
race_year INT,
team_name STRING,
driver_id INT,
driver_name STRING,
race_id INT,
position INT,
points INT,
calculated_points INT,
created_date TIMESTAMP,
updated_date TIMESTAMP)
USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW race_result_updated
AS
SELECT 
races.race_year, 
constructors.name AS team_name, 
drivers.driver_id,
races.race_id,
drivers.name AS driver_name, 
results.position, 
results.points,
11 - results.position AS calculated_points
FROM f1_processed.results
JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
JOIN f1_processed.races ON (results.race_id = races.race_id)
WHERE results.position <= 10 AND results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO f1_presentation.calculated_race_results tgt
USING race_result_updated upd
ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
WHEN MATCHED THEN
UPDATE SET tgt.position = upd.position,
tgt.points = upd.points,
tgt.calculated_points = upd.calculated_points,
tgt.updated_date = current_timestamp
WHEN NOT MATCHED
THEN INSERT (race_year, team_name, driver_name, race_id, driver_id, position, points, calculated_points, created_date)
VALUES (race_year, team_name, driver_name, race_id, driver_id, position, points, calculated_points, current_timestamp)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results