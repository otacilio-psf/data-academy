# Databricks notebook source
# MAGIC %md
# MAGIC # IMDb
# MAGIC 
# MAGIC Subsets of IMDb data are available for access to customers for personal and non-commercial use.
# MAGIC 
# MAGIC [docs](https://www.imdb.com/interfaces/)

# COMMAND ----------

def clean_up():
    dbutils.fs.rm("/mnt/datalake/silver/imdb", True)
    spark.sql(f"DROP DATABASE IF EXISTS silver_imdb CASCADE")
    
clean_up()

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
