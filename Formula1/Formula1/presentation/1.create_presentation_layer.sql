-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dl72/presentation"

-- COMMAND ----------

SELECT * FROM f1_presentation.driver_standings WHERE race_year = 2000