-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create raw tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Create circuits table (CSV)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING CSV
OPTIONS(path "/mnt/formula1dl72/raw/circuits.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create races table (CSV)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date STRING,
  time STRING,
  url STRING
)
USING CSV
OPTIONS(path "/mnt/formula1dl72/raw/races.csv", header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Create constructors table (JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/formula1dl72/raw/constructors.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Create drivers table (JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/formula1dl72/raw/drivers.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Create results table (JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  constructorId INT,
  raceId INT,
  driverId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT,
  resultId INT
)
USING JSON
OPTIONS(path "/mnt/formula1dl72/raw/results.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Create Pit_stops table (JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS(path "/mnt/formula1dl72/raw/pit_stops.json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Create lap times (CSV)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS(path "/mnt/formula1dl72/raw/lap_times")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8. Create qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS(path "/mnt/formula1dl72/raw/qualifying", multiLine True)