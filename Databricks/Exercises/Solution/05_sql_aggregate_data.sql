-- Databricks notebook source
-- MAGIC %md # Set Functions

-- COMMAND ----------

-- MAGIC %md ## Question 01 - The oldest and the newest year for movie from `imdb.title_basics`

-- COMMAND ----------

SELECT 
  MIN(startYear) AS oldest_year,
  MAX(startYear) AS newest_year
FROM imdb.title_basics
WHERE titleType = 'movie'

-- COMMAND ----------

-- MAGIC %md ## Question 02 - How many movies existis of the genre Animation from `imdb.title_basics`

-- COMMAND ----------

SELECT COUNT(1) AS num_animation_movies
FROM imdb.title_basics
WHERE titleType = 'movie' AND genres LIKE '%Animation%'

-- COMMAND ----------

-- MAGIC %md ## Question 03 - Total of votes and the avarege of votes from `imdb.title_ratings`

-- COMMAND ----------

SELECT
  SUM(numVotes) AS total_votes,
  AVG(numVotes) AS avg_votes
FROM imdb.title_ratings

-- COMMAND ----------

-- MAGIC %md # GROUP BY

-- COMMAND ----------

-- MAGIC %md ## Question 04 - How many titles we have by type from `imdb.title_basics`

-- COMMAND ----------

SELECT
  titleType,
  COUNT(1) AS num_titles_type
FROM imdb.title_basics
GROUP BY titleType

-- COMMAND ----------

-- MAGIC %md ## Question 05 - The average runtime and the number of movies divide by start year and older or equal to 2000

-- COMMAND ----------

SELECT
  startYear,
  COUNT(1) AS num_titles_year,
  AVG(runtimeMinutes) AS avg_runtime
FROM imdb.title_basics
WHERE titleType = 'movie' AND startYear > 2000
GROUP BY startYear
ORDER BY startYear

-- COMMAND ----------

-- MAGIC %md ## Question 06 - The average runtime and the number of movies divide by start year and older or equal to 2000 and average runtime its grater or equal to 90

-- COMMAND ----------

SELECT
  startYear,
  COUNT(1) AS num_titles_year,
  AVG(runtimeMinutes) AS avg_runtime
FROM imdb.title_basics
WHERE titleType = 'movie' AND startYear > 2000
GROUP BY startYear
HAVING avg_runtime >= 90
ORDER BY startYear
