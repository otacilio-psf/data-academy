# Databricks notebook source
# MAGIC %md
# MAGIC # IMDb
# MAGIC 
# MAGIC Subsets of IMDb data are available for access to customers for personal and non-commercial use.
# MAGIC 
# MAGIC [docs](https://www.imdb.com/interfaces/)

# COMMAND ----------

studant_name = ""

# COMMAND ----------

def clean_up():
    dbutils.fs.rm(f"/mnt/datalake/studants/{studant_name}/silver/imdb", True)
    spark.sql(f"DROP DATABASE IF EXISTS silver_{studant_name}_imdb CASCADE")
    
clean_up()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest as Delta silver table on Hive

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS silver_{studant_name}_imdb")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Rules
# MAGIC 
# MAGIC - title_basics:
# MAGIC   - columns names should be all in [snake_case](https://en.wikipedia.org/wiki/Snake_case)
# MAGIC   - column is_adult need to be a flag Y/N, null values or diff them 0 or 1 should be Y (better yes then n)
# MAGIC   - column is_adult renamed for is_adult_flag
# MAGIC   - columns start_year and end_year need to be integer
# MAGIC   - column runtime_minutes need to be integer
# MAGIC   - change '\N' from genres to 'Unknown'
# MAGIC   - extract main_genre need to be the first genre of genres
# MAGIC   
# MAGIC - title_ratings :
# MAGIC   - columns names should be all in [snake_case](https://en.wikipedia.org/wiki/Snake_case)
# MAGIC   - column average_rating need to be float
# MAGIC   - column num_votes need to be integer
# MAGIC   - create num_votes_type
# MAGIC     - 0 > num_votes >= 500 -> unexplored
# MAGIC     - 500 > num_votes > 2500 - known
# MAGIC     - 2500 > num_votes > trending
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_imdb.title_basics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_imdb.title_ratings 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Call Funtions

# COMMAND ----------

from pyspark.sql.functions import col, lit, when, split, count
import re

# function to change name
def fix_name(df):
    for _c in df.columns:
        _c_new = _c
        for _l in re.findall("[A-Z]", _c):
            _c_new = _c_new.replace(_l, f"_{_l.lower()}")

        df = df.withColumnRenamed(_c, _c_new)
        
    return df
  
def store_and_mount(df, db_table):
    
    split_db_table = db_table.split(".")
    table_path = split_db_table[0].replace("_", "/")
    t_name = split_db_table[1]
    
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(f'/mnt/datalake/{table_path}/{t_name}')
    
    spark.sql(f"DROP TABLE IF EXISTS {db_table}")
    spark.sql(f"CREATE TABLE {db_table} USING DELTA LOCATION '/mnt/datalake/{table_path}/{t_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_imdb.title_basics

# COMMAND ----------

# <--- Developement --->

# COMMAND ----------

store_and_mount(df_title_basics, f"silver_{studant_name}_imdb.title_basics")

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_imdb.title_ratings

# COMMAND ----------

# <--- Developement --->

# COMMAND ----------

store_and_mount(df_title_ratings, f"silver_{studant_name}_imdb.title_ratings")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funtions sumary
# MAGIC 
# MAGIC Documentation functions:
# MAGIC - [withColumnRenamed](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.withColumnRenamed.html)
# MAGIC - [printSchema](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.printSchema.html)
# MAGIC - [withColumn](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html)
# MAGIC - [col](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.col.html)
# MAGIC - [lit](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.lit.html)
# MAGIC - [cast](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.Column.cast.html)
# MAGIC - [when](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.when.html)
# MAGIC - [isNull](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.Column.isNull.html)
# MAGIC - [filter](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.filter.html)
# MAGIC - [limit](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.limit.html)
