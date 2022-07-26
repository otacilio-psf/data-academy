-- Databricks notebook source
-- MAGIC %md # WHERE
-- MAGIC 
-- MAGIC - =
-- MAGIC - <>
-- MAGIC - \>
-- MAGIC - <
-- MAGIC - \>=
-- MAGIC - <=
-- MAGIC - LIKE
-- MAGIC - IN
-- MAGIC - IS
-- MAGIC - NOT

-- COMMAND ----------

-- MAGIC %md ## Question 1 - Return all movies from `imdb.title_basics`

-- COMMAND ----------

SELECT *
FROM imdb.title_basics
WHERE titleType = 'movie'

-- COMMAND ----------

-- MAGIC %md ## Question 2 - Return all titles with average rating is larger or equal to 8 from `imdb.title_ratings`

-- COMMAND ----------

SELECT *
FROM imdb.title_ratings
WHERE averageRating >= 8

-- COMMAND ----------

-- MAGIC %md ## Question 3 - Return all distinct `genres` for video games from `imdb.title_basics`

-- COMMAND ----------

SELECT DISTINCT genres
FROM imdb.title_basics
WHERE titleType = 'videoGame'

-- COMMAND ----------

-- MAGIC %md ## Question 4 - Return all tv series where one of the genres are Crime from `imdb.title_basics`

-- COMMAND ----------

SELECT *
FROM imdb.title_basics
WHERE titleType = 'tvSeries' AND genres LIKE '%Crime%'

-- COMMAND ----------

-- MAGIC %md ## Question 5 - Return all titles that its not short, tv short and video game from `imdb.title_basics`

-- COMMAND ----------

SELECT *
FROM imdb.title_basics
WHERE titleType NOT IN ('short', 'tvShort', 'videoGame')

-- COMMAND ----------

-- MAGIC %md ## Question 6 - Return all titles where average rating is less then 5 or number of votes is bigger then 30000 from `imdb.title_ratings`

-- COMMAND ----------

SELECT *
FROM imdb.title_ratings
WHERE averageRating < 5 OR numVotes > 30000
