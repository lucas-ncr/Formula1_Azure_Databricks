-- Databricks notebook source
SELECT driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
total_points/total_races AS points_per_race
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races > 50
ORDER BY points_per_race DESC

-- COMMAND ----------

SELECT driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
total_points/total_races AS points_per_race
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING total_races > 50
ORDER BY points_per_race DESC

-- COMMAND ----------

SELECT driver_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
total_points/total_races AS points_per_race
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING total_races > 50
ORDER BY points_per_race DESC