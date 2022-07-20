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


-- COMMAND ----------

-- INSERT DATA


-- COMMAND ----------

-- SELECT DATA


-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/user/hive/warehouse/01_data_academy_delta.db'))
-- MAGIC display(dbutils.fs.ls('/user/hive/warehouse/01_data_academy_delta.db/personal_info_<student-name>'))

-- COMMAND ----------

-- DELETE TABLE


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


-- COMMAND ----------

-- INSERT DATA


-- COMMAND ----------

-- SELECT DATA


-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/FileStore/delta-data-academy/<student-name>/departure_delays'))

-- COMMAND ----------

-- DELETE TABLE


-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/FileStore/delta-data-academy/<student-name>/departure_delays'))

-- COMMAND ----------

-- MAGIC %md ## Clean

-- COMMAND ----------

DROP DATABASE 01_data_academy_delta CASCADE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/FileStore/delta-data-academy/<student-name>/departure_delays', True)
