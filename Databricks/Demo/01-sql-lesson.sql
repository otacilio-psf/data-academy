-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # SELECTing Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Basic quey
-- MAGIC 
-- MAGIC - SELECT
-- MAGIC - FROM
-- MAGIC - wildcard *

-- COMMAND ----------

SELECT *
FROM data_sample.agenda

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Aliases
-- MAGIC 
-- MAGIC - AS

-- COMMAND ----------

SELECT
  a.first_name,
  a.last_name,
  a.phone_number AS number
FROM data_sample.agenda AS a

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Unique values
-- MAGIC 
-- MAGIC - DISTINCT

-- COMMAND ----------

SELECT
  DISTINCT a.first_name
FROM data_sample.agenda AS a

-- COMMAND ----------

SELECT
  DISTINCT
    a.first_name,
    a.last_name
FROM data_sample.agenda AS a

-- COMMAND ----------

SELECT
  DISTINCT a.id
FROM data_sample.agenda AS a

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Filtering Data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Boolean operators
-- MAGIC 
-- MAGIC - =
-- MAGIC - <>
-- MAGIC - \>
-- MAGIC - <
-- MAGIC - \>=
-- MAGIC - <=

-- COMMAND ----------

SELECT
  a.first_name,
  a.last_name
FROM data_sample.agenda AS a
WHERE a.first_name = 'Ethan'

-- COMMAND ----------

SELECT
  f.origin,
  f.destination,
  f.distance
FROM data_sample.flight_delays f
WHERE distance >= 4312

-- COMMAND ----------

SELECT
  f.origin,
  f.destination,
  f.distance
FROM data_sample.flight_delays f
WHERE f.distance >= 4312 AND f.destination <> 'JFK'

-- COMMAND ----------

SELECT
  f.delay,
  f.origin,
  f.destination,
  f.distance
FROM data_sample.flight_delays f
WHERE f.delay > 0 AND f.delay < 15

-- COMMAND ----------

SELECT
  f.delay,
  f.origin,
  f.destination,
  f.distance
FROM data_sample.flight_delays f
WHERE f.origin = 'JFK' OR f.destination  = 'JFK'

-- COMMAND ----------

-- MAGIC %md ## Specials operators
-- MAGIC - LIKE
-- MAGIC - IN
-- MAGIC - IS
-- MAGIC - NOT

-- COMMAND ----------

SELECT
  c.customer_id,
  c.state,
  c.city,
  c.street
FROM data_sample.customers c
WHERE street LIKE '%HILL%' 

-- COMMAND ----------

SELECT
  c.customer_id,
  c.state,
  c.city,
  c.street
FROM data_sample.customers c
WHERE c.state IN ('AZ', 'SC', 'MN')

-- COMMAND ----------

SELECT
  c.customer_id,
  c.state,
  c.city,
  c.street
FROM data_sample.customers c
WHERE c.city IS NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Ordering Data
-- MAGIC 
-- MAGIC - ORDER BY
-- MAGIC - ASC
-- MAGIC - DESC

-- COMMAND ----------

SELECT *
FROM data_sample.promo_prices
ORDER BY sales_price

-- COMMAND ----------

SELECT *
FROM data_sample.promo_prices
ORDER BY sales_price DESC

-- COMMAND ----------

SELECT
  customer_id,
  customer_name,
  state,
  units_purchased
FROM data_sample.customers
ORDER BY state ASC, units_purchased DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Aggragations

-- COMMAND ----------

-- MAGIC %md ## Set Functions
-- MAGIC 
-- MAGIC - COUNT
-- MAGIC - MAX
-- MAGIC - MIN
-- MAGIC - AVG
-- MAGIC - SUM

-- COMMAND ----------

SELECT
  COUNT(1) AS count_num_rows,
  COUNT(*) AS count_star,
  COUNT(city) AS count_cities
FROM data_sample.customers

-- COMMAND ----------

SELECT
  MAX(state) AS last_alphab_state,
  MIN(state) AS first_alphab_state,
  MAX(units_purchased) AS max_units_purchased,
  MIN(units_purchased) AS min_units_purchased
FROM data_sample.customers

-- COMMAND ----------

SELECT
  SUM(sales_price) AS sum_sale_price,
  AVG(sales_price) AS avg_sale_price,
  (SUM(sales_price)/COUNT(sales_price)) AS avg_sale_price_calculated
FROM data_sample.promo_prices

-- COMMAND ----------

-- MAGIC %md ## GROUP BY
-- MAGIC 
-- MAGIC - GROUP BY
-- MAGIC - HAVING

-- COMMAND ----------

SELECT *
FROM data_sample.flight_delays

-- COMMAND ----------

SELECT
  origin,
  destination,
  COUNT(1) AS num_itinerary
FROM data_sample.flight_delays
WHERE origin = 'JFK'
GROUP BY origin, destination
ORDER BY num_itinerary DESC

-- COMMAND ----------

SELECT
  origin,
  destination,
  COUNT(1) AS num_itinerary
FROM data_sample.flight_delays
WHERE origin = 'JFK'
GROUP BY
  origin,
  destination
HAVING num_itinerary < 100
ORDER BY num_itinerary DESC

-- COMMAND ----------

-- MAGIC %md # Combine Tables
-- MAGIC 
-- MAGIC - INNER JOIN
-- MAGIC - LEFT JOIN
-- MAGIC - RIGHT JOIN
-- MAGIC - FULL JOIN

-- COMMAND ----------

UPDATE data_sample.customers c
SET c.loyalty_segment = 4
WHERE c.loyalty_segment IS NULL

-- COMMAND ----------

SELECT
  c.customer_id,
  c.customer_name,
  c.state,
  c.loyalty_segment
FROM data_sample.customers c

-- COMMAND ----------

SELECT
  ls.loyalty_segment_id,
  ls.loyalty_segment_description
FROM data_sample.loyalty_segments ls

-- COMMAND ----------

-- MAGIC %md ## INNER JOIN

-- COMMAND ----------

SELECT
  c.customer_id,
  c.customer_name,
  c.state,
  ls.loyalty_segment_description
FROM data_sample.customers c
INNER JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id

-- COMMAND ----------

SELECT DISTINCT c.loyalty_segment, ls.loyalty_segment_description
FROM data_sample.customers c
INNER JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id

-- COMMAND ----------

-- MAGIC %md ## LEFT JOIN

-- COMMAND ----------

SELECT
  c.customer_id,
  c.customer_name,
  c.state,
  ls.loyalty_segment_description
FROM data_sample.customers c
LEFT JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id

-- COMMAND ----------

SELECT DISTINCT c.loyalty_segment, ls.loyalty_segment_description
FROM data_sample.customers c
LEFT JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id

-- COMMAND ----------

-- MAGIC %md ## RIGHT JOIN

-- COMMAND ----------

SELECT
  c.customer_id,
  c.customer_name,
  c.state,
  ls.loyalty_segment_description
FROM data_sample.customers c
RIGHT JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id

-- COMMAND ----------

SELECT DISTINCT c.loyalty_segment, ls.loyalty_segment_description
FROM data_sample.customers c
RIGHT JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id

-- COMMAND ----------

-- MAGIC %md ## FULL JOIN

-- COMMAND ----------

SELECT
  c.customer_id,
  c.customer_name,
  c.state,
  ls.loyalty_segment_description
FROM data_sample.customers c
FULL JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id

-- COMMAND ----------

SELECT DISTINCT c.loyalty_segment, ls.loyalty_segment_description
FROM data_sample.customers c
FULL JOIN data_sample.loyalty_segments ls
  ON c.loyalty_segment = ls.loyalty_segment_id
