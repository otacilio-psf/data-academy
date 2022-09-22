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
    dbutils.fs.rm("/mnt/datalake/silver/imdb", True)
    dbutils.fs.rm("/mnt/datalake/gold/imdb", True)
    spark.sql(f"DROP DATABASE IF EXISTS bronze_imdb CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS silver_imdb CASCADE")
    spark.sql(f"DROP DATABASE IF EXISTS gold_imdb CASCADE")
    
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

tables = ["title.basics", "title.ratings"]

spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze_imdb")

def bronze_ingestion(tab):
    tab_ = tab.replace(".","_")
    
    (
        spark.read.format("csv")
        .options(header=True, sep='\t')
        .load(f"file:/content/raw/{tab}.tsv.gz")
        .write.mode("overwrite").format("delta")
        .save(f"/mnt/datalake/bronze/imdb/{tab_}")
    )
    
    spark.sql(f"DROP TABLE IF EXISTS bronze_imdb.{tab_}")
    spark.sql(f"CREATE TABLE bronze_imdb.{tab_} USING DELTA LOCATION '/mnt/datalake/bronze/imdb/{tab_}'")
    
    print(f"INFO: bronze_imdb.{tab_} created")

for t in tables:
    bronze_ingestion(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest as Delta silver table on Hive

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS silver_imdb")

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
# MAGIC ### Exploring sneak_case solution

# COMMAND ----------

# Read Data
df_title_basics = spark.read.table("bronze_imdb.title_basics")
print(df_title_basics.columns)

# COMMAND ----------

import re

col_name = "titleType"

upper_letters = re.findall("[A-Z]", col_name)
print(upper_letters)

for _l in upper_letters:
    col_name = col_name.replace(_l, f"_{_l.lower()}")
    
print(col_name)

# COMMAND ----------

# Change Column name
original_columns_names = df_title_basics.columns

for _c in original_columns_names:
    _c_new = _c
    for _l in re.findall("[A-Z]", _c):
        _c_new = _c_new.replace(_l, f"_{_l.lower()}")
    
    df_title_basics = df_title_basics.withColumnRenamed(_c, _c_new)
    
print(df_title_basics.columns)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_imdb.title_basics

# COMMAND ----------

df_title_basics = spark.read.table("bronze_imdb.title_basics")
df_title_basics = fix_name(df_title_basics)

# COMMAND ----------

df_title_basics.printSchema()

# COMMAND ----------

df_title_basics = (
    df_title_basics
    .withColumn("is_adult", col("is_adult").cast("integer"))
    .withColumn("is_adult", when(col("is_adult")==0, lit("N")).otherwise(lit("Y")))
    .withColumnRenamed("is_adult", "is_adult_flag")
    .withColumn("start_year", col("start_year").cast("integer"))
    .withColumn("end_year", col("end_year").cast("integer"))
    .withColumn("runtime_minutes", col("runtime_minutes").cast("integer"))
)

# COMMAND ----------

df_title_basics = (
    df_title_basics
    .withColumn("genres", when(col("genres")=="\\N",lit("Unknown")).otherwise(col("genres")))
    .withColumn("main_genre", split(col("genres"), ",")[0])
)

# COMMAND ----------

df_title_basics.filter("is_adult = 'Y'").limit(3).display()
df_title_basics.filter("is_adult = 'N'").limit(3).display()
df_title_basics.filter("main_genre <> genres").limit(3).display()
df_title_basics.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### write result

# COMMAND ----------

df_title_basics.write.mode("overwrite").format("delta").save('/mnt/datalake/silver/imdb/title_basics')

spark.sql("DROP TABLE IF EXISTS silver_imdb.title_basics")
spark.sql("CREATE TABLE silver_imdb.title_basics USING DELTA LOCATION '/mnt/datalake/silver/imdb/title_basics'")

# COMMAND ----------

spark.sql("SELECT * FROM silver_imdb.title_basics").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### write function

# COMMAND ----------

table_name = "silver_imdb.title_basics"
split_table_name = table_name.split(".")
print(split_table_name)
#table_path = split_table_name[0].replace("_", "/")
#t_name = split_table_name[1]
#print(f'/mnt/datalake/{table_path}/{t_name}')

# COMMAND ----------

def store_and_mount(df, db_table):
    
    split_db_table = db_table.split(".")
    table_path = split_db_table[0].replace("_", "/")
    t_name = split_db_table[1]
    
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").save(f'/mnt/datalake/{table_path}/{t_name}')
    
    spark.sql(f"DROP TABLE IF EXISTS {db_table}")
    spark.sql(f"CREATE TABLE {db_table} USING DELTA LOCATION '/mnt/datalake/{table_path}/{t_name}'")

# COMMAND ----------

store_and_mount(df_title_basics, "silver_imdb.title_basics")

# COMMAND ----------

# MAGIC %md
# MAGIC ### silver_imdb.title_ratings

# COMMAND ----------

df_title_ratings = spark.read.table("bronze_imdb.title_ratings")
df_title_ratings = fix_name(df_title_ratings)

# COMMAND ----------

df_title_ratings.printSchema()

# COMMAND ----------

df_title_ratings = (
    df_title_ratings
    .withColumn("average_rating", col("average_rating").cast("float"))
    .withColumn("num_votes", col("num_votes").cast("integer"))
)

# COMMAND ----------

df_title_ratings = (
    df_title_ratings
    .withColumn("num_votes_type",
                when(col("num_votes") > 2500, lit("trending"))
                .when(col("num_votes") > 500, lit("known"))
                .otherwise(lit("unexplored"))
               )
)

# COMMAND ----------

df_title_ratings.groupBy("num_votes_type").agg(count("num_votes_type")).display()
df_title_ratings.printSchema()

# COMMAND ----------

store_and_mount(df_title_ratings, "silver_imdb.title_ratings")

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
df_d_title = df_fact.select('primary_title', 'original_title', 'tconst')

# Make sure they are unique
df_d_title = df_d_title.distinct()

# Create key option 1
df_d_title = df_d_title.withColumn('title_key_1', monotonically_increasing_id())

# Create key option 2
window_spec = Window.partitionBy(lit(1)).orderBy(lit(1))

df_d_title = df_d_title.withColumn('title_key_2', row_number().over(window_spec))

# Select column order (key + other columns)
df_d_title = df_d_title.select('title_key_1', 'title_key_2', 'primary_title', 'original_title', 'tconst')

# COMMAND ----------

df_d_title.orderBy(col('title_key_1').desc()).display()

# COMMAND ----------

# select mapp columns
df_d_title_join = df_d_title.select('tconst', 'title_key_2')

# join with fact
_df_fact = df_fact.join(df_d_title_join, on="tconst", how="left")

# drop old columns
_df_fact = _df_fact.drop('primary_title', 'original_title', 'tconst')

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
d_title_cl = ['primary_title', 'original_title', 'tconst']

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
column_list = ['primary_title', 'original_title', 'tconst']

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
    'start_year',
    'end_year',
    'runtime_minutes',
    'average_rating',
    'num_votes'
)

store_and_mount(df_fact, "gold_imdb.f_rating")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funtions sumary
# MAGIC 
# MAGIC Documentation of all functions used:
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
# MAGIC - [join](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.join.html)
# MAGIC - [distinct](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.distinct.html)
# MAGIC - [monotonically_increasing_id](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html)
# MAGIC - [Window](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.Window.html)
# MAGIC - [row_number](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.row_number.html)
