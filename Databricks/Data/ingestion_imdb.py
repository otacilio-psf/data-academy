# Databricks notebook source
import requests
imdb_tables = [
    {
        "table_name": "title_basics",
        "file_name": "title.basics.tsv.gz",
        "file_url": "https://datasets.imdbws.com/title.basics.tsv.gz",
        "schema": "tconst string, titleType string, primaryTitle string, originalTitle string, isAdult int, startYear int, endYear int, runtimeMinutes float, genres string"
    },
    {
        "table_name": "title_ratings",
        "file_name": "title.ratings.tsv.gz",
        "file_url": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "schema": "tconst string, averageRating float, numVotes int"
    }
]

download_files_path = "/FileStore/otacilio/imdb/"

# COMMAND ----------

def download_file(url, path, file):
    r = requests.get(url, stream=True)
    if r.status_code == requests.codes.OK:
        with open("/tmp/"+file, 'wb') as new_file:
                for part in r.iter_content(chunk_size=2*1024*1024):
                    new_file.write(part)
        dbutils.fs.mv("file:/tmp/"+file, path+file)
    else:
        r.raise_for_status()

# COMMAND ----------

def ingestion(database, table_list, download_files_path):
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    dbutils.fs.mkdirs(download_files_path.replace("/dbfs", ""))
    
    for t in table_list:
        download_file(t["file_url"], download_files_path, t["file_name"])
        (
            spark.read.format("csv").options(header=True, sep='\t').schema(t["schema"]).load(download_files_path+t["file_name"])
            .write.saveAsTable(f"{database}.{t['table_name']}")
        )

# COMMAND ----------

ingestion("imdb", imdb_tables, download_files_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM imdb.title_basics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM imdb.title_ratings
