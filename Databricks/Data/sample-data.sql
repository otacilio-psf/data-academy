-- Databricks notebook source
-- MAGIC %md #Source
-- MAGIC 
-- MAGIC This bases are from Databricks Analyst Learn path

-- COMMAND ----------

CREATE DATABASE data_sample

-- COMMAND ----------

USE data_sample;

DROP TABLE IF EXISTS temp_delays;

DROP TABLE IF EXISTS flight_delays;

CREATE TABLE temp_delays USING CSV OPTIONS (path "wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/flights/departuredelays.csv", header "true", inferSchema "true");

CREATE TABLE flight_delays AS SELECT * FROM temp_delays;

DROP TABLE temp_delays;

 

DROP TABLE IF EXISTS sales;

CREATE TABLE sales AS

      SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales`;

 

DROP TABLE IF EXISTS promo_prices;

CREATE TABLE promo_prices AS

      SELECT * FROM delta.`wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/promo_prices`;

 

DROP TABLE IF EXISTS sales_orders;

CREATE TABLE sales_orders AS

      SELECT * FROM json.`wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/sales_orders`;

 

DROP TABLE IF EXISTS temp_loyalty_segments;

DROP TABLE IF EXISTS loyalty_segments;

CREATE TABLE temp_loyalty_segments USING CSV OPTIONS (path "wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/loyalty_segments/loyalty_segment.csv", header "true", inferSchema "true");

CREATE TABLE loyalty_segments AS SELECT * FROM temp_loyalty_segments;

DROP TABLE temp_loyalty_segments;

 

DROP TABLE IF EXISTS temp_customers;

DROP TABLE IF EXISTS customers;

CREATE TABLE temp_customers USING CSV OPTIONS (path "wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/customers/customers.csv", header "true", inferSchema "true");

CREATE TABLE customers AS SELECT * FROM temp_customers;

DROP TABLE temp_customers;

 

DROP TABLE IF EXISTS temp_suppliers;

DROP TABLE IF EXISTS suppliers;

CREATE TABLE temp_suppliers USING CSV OPTIONS (path "wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/suppliers/suppliers.csv", header "true", inferSchema "true");

CREATE TABLE suppliers AS SELECT * FROM temp_suppliers;

DROP TABLE temp_suppliers;

 

DROP TABLE IF EXISTS temp_suppliers;

DROP TABLE IF EXISTS source_suppliers;

CREATE TABLE temp_suppliers USING CSV OPTIONS (path "wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v01/suppliers/suppliers.csv", header "true", inferSchema "true");

CREATE TABLE source_suppliers AS SELECT * FROM temp_suppliers;

DROP TABLE temp_suppliers;


