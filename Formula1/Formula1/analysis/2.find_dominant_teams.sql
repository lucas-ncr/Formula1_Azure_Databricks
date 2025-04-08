-- Databricks notebook source
DESCRIBE f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT team_name,
SUM(calculated_points) AS total_points,
COUNT(position = 1) AS wins,
race_year
FROM f1_presentation.calculated_race_results
GROUP BY team_name, race_year
ORDER BY total_points DESC

-- COMMAND ----------

SELECT team_name,
SUM(calculated_points) AS total_points,
COUNT(position = 1) AS wins
FROM f1_presentation.calculated_race_results
WHERE race_year >= 2001 AND race_year <= 2011
GROUP BY team_name
ORDER BY total_points DESC