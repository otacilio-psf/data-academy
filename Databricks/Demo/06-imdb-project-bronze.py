# Databricks notebook source
# MAGIC %md
# MAGIC # IMDb
# MAGIC 
# MAGIC Subsets of IMDb data are available for access to customers for personal and non-commercial use.
# MAGIC 
# MAGIC Each dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set. The first line in each file contains headers that describe what is in each column. A ‘\N’ is used to denote that a particular field is missing or null for that title/name
# MAGIC 
# MAGIC [docs](https://www.imdb.com/interfaces/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare environment

# COMMAND ----------

def clean_up():
    dbutils.fs.rm("/mnt/datalake/bronze/imdb", True)
    spark.sql(f"DROP DATABASE IF EXISTS bronze_imdb CASCADE")
    
clean_up()

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /content/raw
# MAGIC wget -q https://datasets.imdbws.com/title.basics.tsv.gz -P /content/raw
# MAGIC wget -q https://datasets.imdbws.com/title.ratings.tsv.gz -P /content/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest as Delta bronze table on Hive

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

tables = ["title.basics", "title.ratings"]

spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze_imdb")

def bronze_ingestion(tab):
    tab_ = tab.replace(".","_")
    
    (
        spark.read.format("csv")
        .options(header=True, sep='\t')
        .load(f"file:/content/raw/{tab}.tsv.gz")
        .withColumn("LOAD_DATETIME", current_timestamp())
        .write.mode("overwrite").format("delta")
        .save(f"/mnt/datalake/bronze/imdb/{tab_}")
    )
    
    spark.sql(f"DROP TABLE IF EXISTS bronze_imdb.{tab_}")
    spark.sql(f"CREATE TABLE bronze_imdb.{tab_} USING DELTA LOCATION '/mnt/datalake/bronze/imdb/{tab_}'")
    
    print(f"INFO: bronze_imdb.{tab_} created")

for t in tables:
    bronze_ingestion(t)
