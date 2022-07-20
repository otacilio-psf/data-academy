-- Databricks notebook source
-- MAGIC %md ## Create Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS 01_data_academy_delta

-- COMMAND ----------

-- MAGIC %md ## Create Managed table
-- MAGIC 
-- MAGIC - Crate Table
-- MAGIC - Insert Data
-- MAGIC - SELECT Table
-- MAGIC - Check files at '/user/hive/warehouse/01_data_academy_delta.db'
-- MAGIC - Delete Table
-- MAGIC - Check files again

-- COMMAND ----------

-- CREATE TABLE
CREATE TABLE IF NOT EXISTS 01_data_academy_delta.personal_info_otacilio
(
    student_first_name string,
    student_last_name string,
    student_age int
)
USING DELTA

-- COMMAND ----------

-- INSERT DATA
INSERT INTO 01_data_academy_delta.personal_info_otacilio
VALUES
    ('otacilio', 'filho', 30)

-- COMMAND ----------

-- SELECT DATA
SELECT *
FROM 01_data_academy_delta.personal_info_otacilio

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/user/hive/warehouse/01_data_academy_delta.db'))
-- MAGIC display(dbutils.fs.ls('/user/hive/warehouse/01_data_academy_delta.db/personal_info_otacilio'))

-- COMMAND ----------

-- DELETE TABLE
DROP TABLE 01_data_academy_delta.personal_info_otacilio

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/user/hive/warehouse/01_data_academy_delta.db'))

-- COMMAND ----------

-- MAGIC %md ## Unmanaged table
-- MAGIC 
-- MAGIC - Crate Table
-- MAGIC - Insert Data
-- MAGIC - SELECT Table
-- MAGIC - Check files at ''
-- MAGIC - Delete Table
-- MAGIC - Check files again

-- COMMAND ----------

-- CREATE TABLE
CREATE TABLE IF NOT EXISTS 01_data_academy_delta.departure_delays_otacilio
(
  date date,
  delay int,
  distance bigint,
  origin string,
  destination string
)
USING DELTA
LOCATION '/FileStore/delta-data-academy/otacilio/departure_delays'

-- COMMAND ----------

-- INSERT DATA
INSERT INTO 01_data_academy_delta.departure_delays_otacilio
SELECT * FROM 01_data_academy_sample.departure_delays

-- COMMAND ----------

-- SELECT DATA
SELECT *
FROM 01_data_academy_delta.departure_delays_otacilio

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/FileStore/delta-data-academy/otacilio/departure_delays'))

-- COMMAND ----------

-- DELETE TABLE
DROP TABLE 01_data_academy_delta.departure_delays_otacilio

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/FileStore/delta-data-academy/otacilio/departure_delays'))

-- COMMAND ----------

-- MAGIC %md ## Clean

-- COMMAND ----------

DROP DATABASE 01_data_academy_delta CASCADE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/FileStore/delta-data-academy/otacilio/departure_delays', True)
