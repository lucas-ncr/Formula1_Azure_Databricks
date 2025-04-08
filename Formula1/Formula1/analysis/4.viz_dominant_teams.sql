-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:blue;text-align:center;">F1 Dominant Teams Dashboard</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE VIEW v_dominant_teams
AS
SELECT team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS points_per_race,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
ORDER BY total_points DESC

-- COMMAND ----------

CREATE OR REPLACE TABLE t_dominant_teams_all_time
AS
SELECT race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS points_per_race,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams LIMIT 5)
GROUP BY team_name, race_year
ORDER BY points_per_race DESC

-- COMMAND ----------

SELECT race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS points_per_race,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams LIMIT 5)
GROUP BY team_name, race_year
ORDER BY points_per_race DESC

-- COMMAND ----------

SELECT race_year,
team_name,
COUNT(1) AS total_races,
SUM(calculated_points) AS total_points,
AVG(calculated_points) AS points_per_race,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calculated_race_results
WHERE team_name IN (SELECT team_name FROM v_dominant_teams LIMIT 5)
GROUP BY team_name, race_year
ORDER BY points_per_race DESC