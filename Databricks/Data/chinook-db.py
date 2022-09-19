# Databricks notebook source
# MAGIC %md
# MAGIC # Chinook
# MAGIC Chinook database is an alternative to the Northwind database, being ideal for demos and testing
# MAGIC 
# MAGIC ![Chinook db](https://raw.githubusercontent.com/otacilio-psf/data-academy/main/.attachments/chinook-db-model.png)
# MAGIC 
# MAGIC [Github](https://github.com/lerocha/chinook-database)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare environment

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /content
# MAGIC mkdir /content/raw
# MAGIC wget -q https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite -P /content

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract SQLite data

# COMMAND ----------

import pandas as pd
import sqlite3

con = sqlite3.connect("/content/Chinook_Sqlite.sqlite")
tables = ['Invoice','Album','Artist','Customer','Employee','Genre','InvoiceLine','MediaType','Playlist','PlaylistTrack','Track']

def extract_sqlite(table):
    pd.read_sql(sql=f'SELECT * FROM {table}', con=con).to_csv(f"/content/raw/{table.lower()}.csv", index=False)

for t in tables:
    extract_sqlite(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest as Delta table on Hive

# COMMAND ----------

def bronze_ingestion(tab):
    tab = tab.lower()
    
    (
        spark.read.format("csv")
        .option("header", "true")
        .load(f"file:/content/raw/{tab}.csv")
        .write.mode("overwrite").format("delta")
        .save(f"/mnt/datalake/bronze/{tab}")
    )
    
    spark.sql(f"DROP TABLE IF EXISTS bronze_chinook.{tab}")
    spark.sql(f"CREATE TABLE bronze_chinook.{tab} LOCATION '/mnt/datalake/bronze/{tab}'")
    
    print(f"INFO: bronze_chinook.{tab} created")

# COMMAND ----------

for t in tables:
    bronze_ingestion(t)
