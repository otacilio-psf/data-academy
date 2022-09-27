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
    dbutils.fs.rm(f"/mnt/datalake/gold/{studant_name}/imdb", True)
    spark.sql(f"DROP DATABASE IF EXISTS gold_{studant_name}_imdb CASCADE")
    
clean_up()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest as Delta gold table on Hive

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS gold_{studant_name}_imdb")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Rules
# MAGIC 
# MAGIC In our case gold layer need to be [Star Schema](https://learn.microsoft.com/en-us/power-bi/guidance/star-schema) to improve performance on Power BI
# MAGIC 
# MAGIC Gold Layer data model:
# MAGIC 
# MAGIC ![IMDb Star Schema](https://raw.githubusercontent.com/otacilio-psf/data-academy/main/.attachments/imdb-star-model.png)

# COMMAND ----------

spark.read.table(f"silver_{studant_name}_imdb.title_basics").display()
spark.read.table(f"silver_{studant_name}_imdb.title_ratings").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Call functions

# COMMAND ----------

from pyspark.sql.functions import row_number, lit
from pyspark.sql import Window

def prepare_dimenssion(df, key_name, column_list):
    window_spec = Window.partitionBy(lit(1)).orderBy(lit(1))
    
    return (
        df
        .select(*column_list)
        .distinct()
        .withColumn(key_name, row_number().over(window_spec))
        .select(key_name, *column_list)
    )
    
def change_fact_dataframe(f_df, d_df, f_key, d_key, column_list):
    d_df = d_df.select(f_key, d_key)
    return (
        f_df
        .join(d_df, on=f_key, how="left")
        .drop(*column_list)
    )

def store_and_mount(df, db_table):
    
    split_db_table = db_table.split(".")
    table_path = split_db_table[0].replace("_", "/")
    t_name = split_db_table[1]
    
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(f'/mnt/datalake/{table_path}/{t_name}')
    
    spark.sql(f"DROP TABLE IF EXISTS {db_table}")
    spark.sql(f"CREATE TABLE {db_table} USING DELTA LOCATION '/mnt/datalake/{table_path}/{t_name}'")

# COMMAND ----------

# <--- Development --->

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Title

# COMMAND ----------

# <--- Development --->

store_and_mount(df_d_title, f"gold_{studant_name}_imdb.d_title")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Title Type

# COMMAND ----------

# <--- Development --->

store_and_mount(df_d_title_type, f"gold_{studant_name}_imdb.d_title_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Main Genre

# COMMAND ----------

# <--- Development --->

store_and_mount(df_d_main_genre, f"gold_{studant_name}_imdb.d_main_genre")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Num Votes Type

# COMMAND ----------

# <--- Development --->

store_and_mount(df_d_num_votes_type, f"gold_{studant_name}_imdb.d_num_votes_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Is Adult

# COMMAND ----------

# <--- Development --->

store_and_mount(df_d_is_adult, f"gold_{studant_name}_imdb.d_is_adult")

# COMMAND ----------

# MAGIC %md
# MAGIC ### F Rating

# COMMAND ----------

# <--- Development --->

store_and_mount(df_fact, f"gold_{studant_name}_imdb.f_rating")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funtions sumary
# MAGIC 
# MAGIC Documentation of functions:
# MAGIC - [join](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.join.html)
# MAGIC - [distinct](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.distinct.html)
# MAGIC - [monotonically_increasing_id](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html)
# MAGIC - [Window](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.Window.html)
# MAGIC - [row_number](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.row_number.html)
