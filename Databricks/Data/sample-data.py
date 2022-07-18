# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE 01_data_academy_sample

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/flights'))

# COMMAND ----------

df_airport_codes = spark.read.format("csv").options(delimiter="\t", header=True).load('/databricks-datasets/flights/airport-codes-na.txt')

df_departure_delays = spark.read.format("csv").options(header=True).load('/databricks-datasets/flights/departuredelays.csv')

# COMMAND ----------

df_airport_codes.write.saveAsTable("01_data_academy_sample.airport_codes")
df_departure_delays.write.saveAsTable("01_data_academy_sample.departure_delays")
