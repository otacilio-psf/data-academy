-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SELECT
-- MAGIC 
-- MAGIC Select the tables from imdb database:
-- MAGIC 
-- MAGIC - imdb.title_basics
-- MAGIC - imdb.title_ratings

-- COMMAND ----------

-- MAGIC %md ## Select All Columns

-- COMMAND ----------

--imdb.title_basics
SELECT *
FROM imdb.title_basics

-- COMMAND ----------

--imdb.title_ratings
SELECT *
FROM imdb.title_ratings

-- COMMAND ----------

-- MAGIC %md ## Select first 3 Columns from each table

-- COMMAND ----------

--imdb.title_basics
SELECT
  tconst,
  titleType,
  primaryTitle
FROM imdb.title_basics

-- COMMAND ----------

--imdb.title_ratings
SELECT
  tconst,
  averageRating,
  numVotes
FROM imdb.title_ratings

-- COMMAND ----------

-- MAGIC %md ## Select distinct values for `titleType` on `imdb.title_basics`

-- COMMAND ----------

--imdb.title_basics
SELECT DISTINCT titleType
FROM imdb.title_basics
