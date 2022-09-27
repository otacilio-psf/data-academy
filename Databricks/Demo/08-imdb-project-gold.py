# Databricks notebook source
# MAGIC %md
# MAGIC # IMDb
# MAGIC 
# MAGIC Subsets of IMDb data are available for access to customers for personal and non-commercial use.
# MAGIC 
# MAGIC [docs](https://www.imdb.com/interfaces/)

# COMMAND ----------

def clean_up():
    dbutils.fs.rm("/mnt/datalake/gold/imdb", True)
    spark.sql(f"DROP DATABASE IF EXISTS gold_imdb CASCADE")
    
clean_up()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest as Delta gold table on Hive

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gold_imdb")

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

from pyspark.sql.functions import monotonically_increasing_id, row_number, lit, col
from pyspark.sql import Window

# COMMAND ----------

df_basic = spark.read.table("silver_imdb.title_basics")
df_ratings = spark.read.table("silver_imdb.title_ratings")

df_fact = df_basic.join(df_ratings, on="tconst", how="inner")
print(df_fact.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manual steps

# COMMAND ----------

# Select dimension columns
df_d_title = df_fact.select('primary_title', 'original_title', 'start_year', 'end_year', 'runtime_minutes')

# Make sure they are unique
df_d_title = df_d_title.distinct()

# Create key option 1
df_d_title = df_d_title.withColumn('title_key_1', monotonically_increasing_id())

# Create key option 2
window_spec = Window.partitionBy(lit(1)).orderBy(lit(1))

df_d_title = df_d_title.withColumn('title_key_2', row_number().over(window_spec))

# Select column order (key + other columns)
df_d_title = df_d_title.select('title_key_1', 'title_key_2', 'primary_title', 'original_title', 'start_year', 'end_year', 'runtime_minutes', 'tconst')

# COMMAND ----------

df_d_title.orderBy(col('title_key_1').desc()).display()

# COMMAND ----------

# select mapp columns
df_d_title_join = df_d_title.select('tconst', 'title_key_2')

# join with fact
_df_fact = df_fact.join(df_d_title_join, on="tconst", how="left")

# drop old columns
_df_fact = _df_fact.drop('primary_title', 'original_title', 'start_year', 'end_year', 'runtime_minutes', 'tconst')

# COMMAND ----------

_df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function abstraction

# COMMAND ----------

def prepare_dimenssion(df, key_name, column_list):
    window_spec = Window.partitionBy(lit(1)).orderBy(lit(1))
    
    return (
        df
        .select(*column_list)
        .distinct()
        .withColumn(key_name, row_number().over(window_spec))
        .select(key_name, *column_list)
    )

# COMMAND ----------

def change_fact_dataframe(f_df, d_df, f_key, d_key, column_list):
    d_df = d_df.select(f_key, d_key)
    return (
        f_df
        .join(d_df, on=f_key, how="left")
        .drop(*column_list)
    )

# COMMAND ----------

dimension_key = "title_key"
d_title_cl = ['primary_title', 'original_title', 'start_year', 'end_year', 'runtime_minutes', 'tconst']

df_d_title = prepare_dimenssion(df_fact, dimension_key, d_title_cl)

df_fact = change_fact_dataframe(df_fact, df_d_title, 'tconst', dimension_key, d_title_cl)

# COMMAND ----------

df_d_title = df_d_title.drop('tconst')

df_d_title.display()
df_fact.display()

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

df_basic = spark.read.table("silver_imdb.title_basics")
df_ratings = spark.read.table("silver_imdb.title_ratings")

df_fact = df_basic.join(df_ratings, on="tconst", how="inner")
print(df_fact.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Title

# COMMAND ----------

dimension_key = "title_key"
column_list = ['primary_title', 'original_title', 'start_year', 'end_year', 'runtime_minutes', 'tconst']

df_d_title = prepare_dimenssion(df_fact, dimension_key, column_list)

df_fact = change_fact_dataframe(df_fact, df_d_title, 'tconst', dimension_key, column_list)

df_d_title = df_d_title.drop('tconst')

store_and_mount(df_d_title, "gold_imdb.d_title")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Title Type

# COMMAND ----------

dimension_key = "title_type_key"
column_list = ['title_type']

df_d_title_type = prepare_dimenssion(df_fact, dimension_key, column_list)

df_fact = change_fact_dataframe(df_fact, df_d_title_type, 'title_type', dimension_key, column_list)

store_and_mount(df_d_title_type, "gold_imdb.d_title_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Main Genre

# COMMAND ----------

dimension_key = "main_genre_key"
column_list = ['main_genre']

df_d_main_genre = prepare_dimenssion(df_fact, dimension_key, column_list)

df_fact = change_fact_dataframe(df_fact, df_d_main_genre, 'main_genre', dimension_key, column_list)

store_and_mount(df_d_main_genre, "gold_imdb.d_main_genre")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Num Votes Type

# COMMAND ----------

dimension_key = "num_votes_type_key"
column_list = ['num_votes_type']

df_d_num_votes_type = prepare_dimenssion(df_fact, dimension_key, column_list)

df_fact = change_fact_dataframe(df_fact, df_d_num_votes_type, 'num_votes_type', dimension_key, column_list)

store_and_mount(df_d_num_votes_type, "gold_imdb.d_num_votes_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ### D Is Adult

# COMMAND ----------

dimension_key = "is_adult_key"
column_list = ['is_adult_flag']

df_d_is_adult = prepare_dimenssion(df_fact, dimension_key, column_list)

df_fact = change_fact_dataframe(df_fact, df_d_is_adult, 'is_adult_flag', dimension_key, column_list)

store_and_mount(df_d_is_adult, "gold_imdb.d_is_adult")

# COMMAND ----------

# MAGIC %md
# MAGIC ### F Rating

# COMMAND ----------

df_fact.display()

# COMMAND ----------

df_fact = df_fact.select(
    'title_key',
    'title_type_key',
    'main_genre_key',
    'num_votes_type_key',
    'is_adult_key',
    'average_rating',
    'num_votes'
)

store_and_mount(df_fact, "gold_imdb.f_rating")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results

# COMMAND ----------

spark.sql("SELECT * FROM gold_imdb.d_is_adult").display()

# COMMAND ----------

spark.sql("SELECT * FROM gold_imdb.d_main_genre").display()

# COMMAND ----------

spark.sql("SELECT * FROM gold_imdb.d_num_votes_type").display()

# COMMAND ----------

spark.sql("SELECT * FROM gold_imdb.d_title").display()

# COMMAND ----------

spark.sql("SELECT * FROM gold_imdb.d_title_type").display()

# COMMAND ----------

spark.sql("SELECT * FROM gold_imdb.f_rating").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Funtions sumary
# MAGIC 
# MAGIC Documentation of all functions used:
# MAGIC - [join](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.join.html)
# MAGIC - [distinct](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.distinct.html)
# MAGIC - [monotonically_increasing_id](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html)
# MAGIC - [Window](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.Window.html)
# MAGIC - [row_number](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.row_number.html)
