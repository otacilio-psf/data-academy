# Databricks notebook source
# MAGIC %md
# MAGIC # Schema
# MAGIC 
# MAGIC A schema in Spark is a specification of column types in a DataFrame. They are used when reading external data and creating DataFrames, and can be passed directly to Spark or can be inferred. Passing a schema in the reading has interesting benefits, such as:
# MAGIC 
# MAGIC - Prevents Spark from performing type inference, which is costly and time-consuming depending on file size, as well as error-prone;
# MAGIC - Allows the user to identify errors in the data as soon as it is read, if the data does not follow the specified schema.

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Dataframe
# MAGIC 
# MAGIC We can create a dataframe from a file (common way) or we can create it in run time (test or small data)
# MAGIC 
# MAGIC ```
# MAGIC df = spark.createDataFrame(data, schema)
# MAGIC ```

# COMMAND ----------

data = [
    ('Otacilio', 1),
    ('Pedro', 2),
    ('Andre', 3),
    ('Maria', 4)
]

df = spark.createDataFrame(data, schema = ['name', 'id'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Schema with SQL DDL sintax

# COMMAND ----------

data = [
    ('Otacilio', 1),
    ('Pedro', 2),
    ('Andre', 3),
    ('Maria', 4)
]

schema = "name string, id int"
df = spark.createDataFrame(data, schema = schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define a Schema with PySpark lib

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

schema = StructType(
    [
        StructField('name', StringType()),
        StructField('id', IntegerType())
    ]
)

data = []

for i in range(100):
    data.append(('Otacilio', i))

df = spark.createDataFrame(data, schema = schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data
# MAGIC 
# MAGIC Read data with spark follows the same pattern for different formats
# MAGIC 
# MAGIC spark.read.format(format).option(paramenter).load(file/path)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake/sample"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV
# MAGIC CSV stand for comma-separated values, but the separator can be different:
# MAGIC   - ;
# MAGIC   - \t -> tab
# MAGIC   - | -> pipe

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/datalake/sample/profile_csv.csv")
df.display()

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", True)
    .load("/mnt/datalake/sample/profile_csv.csv")
)

df.printSchema()
df.display()

# COMMAND ----------

schema = "username string, name string, sex int, addres string, mail string, birthdate date"

df = (
    spark.read.format("csv")
    .options(header=True, sep=";")
    .schema(schema)
    .load("/mnt/datalake/sample/profile_csv.csv")
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON
# MAGIC JavaScript Object Notation - Despite the name JavaScript Object Notation, JSON is independent of any programming language and is a common API output in a wide variety of applications. In python we represent as dictionaries

# COMMAND ----------

df = (
    spark.read.format("json")
    .option("inferSchema", True)
    .load("/mnt/academy_datalake/sample/json")
)
df.display()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake/sample/json"))

# COMMAND ----------

df = (
    spark.read.format("json")
    .option("inferSchema", True)
    .load("/mnt/datalake/sample/json/profile_000.json")
)
schema = df.schema
print(schema)

# COMMAND ----------

df = (
    spark.read.format("json")
    .schema(schema)
    .load("/mnt/datalake/sample/json")
)
df.display()

# COMMAND ----------

dbutils.fs.head("dbfs:/mnt/datalake/sample/json/profile_016.json")

# COMMAND ----------

schema = "username string, name string, sex int, addres string, mail string, birthdate date"

df = (
    spark.read.format("json")
    .schema(schema)
    .load("/mnt/datalake/sample/json")
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet
# MAGIC Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. Is the best format for Spark!

# COMMAND ----------

df = (
    spark.read.format("parquet")
    .load("/mnt/datalake/sample/profile_parquet.parquet")
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Excel spreedshet
# MAGIC It's not good or usual to have xls in our database but sometimes is inevitable, so how we can read it?
# MAGIC 
# MAGIC Spark core it's not compatible with excel files but since Spark 3.2 we have pandas API that allow us to do it

# COMMAND ----------

# MAGIC %pip install xlrd openpyxl 

# COMMAND ----------

import pyspark.pandas as pd

# COMMAND ----------

df = pd.read_excel("/mnt/datalake/sample/profile_excel.xls").to_spark()

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Data
# MAGIC 
# MAGIC To write our DataFrame on the disk we will use the sintax
# MAGIC 
# MAGIC df.write.mode(mode).format(format).option(option).save(/datalake/path)
# MAGIC 
# MAGIC mode:
# MAGIC   - append
# MAGIC   - overwrite
# MAGIC   - errorifexists - default
# MAGIC   - ignore
# MAGIC 
# MAGIC format - core:
# MAGIC   - json
# MAGIC   - csv
# MAGIC   - parquet
# MAGIC   
# MAGIC format - third party:
# MAGIC   - [delta](https://delta.io/)

# COMMAND ----------

schema = "username string, name string, sex string, address string, mail string, birthdate date"
df = spark.read.format("json").schema(schema).load("/mnt/datalake/sample/json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV
# MAGIC 
# MAGIC 
# MAGIC [options](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrameWriter.csv.html):
# MAGIC   - header
# MAGIC   - sep
# MAGIC   - nullValue

# COMMAND ----------

df.write.mode("overwrite").format("csv").save("/FileStore/otacilio/spark-lesson/csv_1")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/otacilio/spark-lesson/csv_1"))

# COMMAND ----------

spark.read.format('csv').option("header", True).load("/FileStore/otacilio/spark-lesson/csv_1").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data partitions 
# MAGIC 
# MAGIC As Spark divide the data in partitions when we are writing it will create a file output for each partition.
# MAGIC 
# MAGIC Exist a common performance issue with Spark called "small files problem", the optimize size for each file is arround 200 mb, as with Delta Lake we have a good tool to solve this kind of issue we won't discuss strategies to solve it.
# MAGIC 
# MAGIC Some times we want to write only one CSV to download a sample of data for it we will use the function [coalesce](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.coalesce.html)(1) to reduce the number of partitions to 1
# MAGIC   - <img src="https://cdn-icons-png.flaticon.com/512/564/564619.png" alt="warn" width="25"/> It is for small data, you can crash the cluster if try to put to much data in one partition

# COMMAND ----------

(
    df.coalesce(1).write
    .mode("overwrite")
    .format("csv")
    .option("header", True)
    .save("/FileStore/otacilio/spark-lesson/csv_2")
)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/otacilio/spark-lesson/csv_2"))

# COMMAND ----------

spark.read.format('csv').option("header", True).load("/FileStore/otacilio/spark-lesson/csv_2").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet
# MAGIC 
# MAGIC Parquet keeps data and metadata.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Folder partition:
# MAGIC It's not specific from parquet but we can [partition by](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrameWriter.partitionBy.html#pyspark.sql.DataFrameWriter.partitionBy) a column when we are writing (!= data partition on spark memory)
# MAGIC   - Partition helps with data skipping, i.e., when the partition is used on filters Spark will read only the necessay ones
# MAGIC   - <img src="https://cdn-icons-png.flaticon.com/512/497/497738.png" alt="warn" width="25"/> A bad partition strategy can be worst them no partiton, Delta Lake have optimizations for data skipping
# MAGIC   

# COMMAND ----------

df.write.mode("overwrite").format("parquet").save("/FileStore/otacilio/spark-lesson/parquet_1")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/otacilio/spark-lesson/parquet_1"))

# COMMAND ----------

spark.read.format("parquet").load("/FileStore/otacilio/spark-lesson/parquet_1").display()

# COMMAND ----------

df.write.mode("overwrite").format("parquet").partitionBy('sex').save("/FileStore/otacilio/spark-lesson/parquet_2")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/otacilio/spark-lesson/parquet_2/"))

# COMMAND ----------

spark.read.format("parquet").load("/FileStore/otacilio/spark-lesson/parquet_2").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Operations

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/datalake/sample/profile_parquet.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### show and display

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### printSchema

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### columns

# COMMAND ----------

print(df.columns)
print(type(df.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ### select and drop column

# COMMAND ----------

df_result = df.select("username", "sex", "birthdate")
df_result.display()

# COMMAND ----------

df_result = df_result.drop("birthdate")
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df_result = df.withColumnRenamed("name", "full_name")
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### col and lit
# MAGIC 
# MAGIC col:
# MAGIC  - Is the object that represent a column from DataFrame
# MAGIC 
# MAGIC lit:
# MAGIC  - Is the obeject that represent a column with constant value

# COMMAND ----------

# MAGIC %md
# MAGIC ### cast
# MAGIC 
# MAGIC it is a column method where we can cast a new type

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn
# MAGIC 
# MAGIC Generate or change a column

# COMMAND ----------

from pyspark.sql.functions import col, lit, split

# COMMAND ----------

df_result = df.withColumn("mail_domain", split(col("mail"), "@")[1])
df_result = df_result.withColumn("birthdate", col("birthdate").cast("timestamp"))
df_result = df_result.withColumn("new_column", lit("academy"))
df_result.display()

# COMMAND ----------

df = (
    df
    .withColumn("mail_domain", split(col("mail"), "@")[1])
    .withColumn("birthdate", col("birthdate").cast("timestamp"))
    .withColumn("new_column", lit("academy"))
)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filter
# MAGIC 
# MAGIC operators
# MAGIC  - and : &
# MAGIC  - or : |
# MAGIC  - not : ~
# MAGIC  - equal : ==
# MAGIC  - different : !=
# MAGIC  - greater than: >
# MAGIC  - less than : <
# MAGIC  
# MAGIC With col() we have another operators:
# MAGIC 
# MAGIC  - isin()
# MAGIC  - contains()
# MAGIC  - like()
# MAGIC  - startswith()
# MAGIC  - endswith()
# MAGIC  - between()
# MAGIC  - isNull()
# MAGIC  - isNotNull()
# MAGIC  
# MAGIC We can also use sql expression in an string
# MAGIC `"col1 == 2"`

# COMMAND ----------

df.filter(col("sex")=='M').display()

# COMMAND ----------

df_result = df.filter(col("mail_domain").isin("gmail.com", "hotmail.com"))
df_result.display()

# COMMAND ----------

df_result = df.filter((col("mail_domain")=="hotmail.com")|(col("sex")=="F"))
df_result.display()

# COMMAND ----------

condition = (col("mail_domain")=="hotmail.com")|(col("sex")=="F")
df_result = df.filter(condition)
df_result.select("sex", "mail_domain").display()

# COMMAND ----------

condition = (col("mail_domain")=="hotmail.com")&(col("sex")=="F")
df_result = df.filter(condition)
df_result.select("sex", "mail_domain").display()

# COMMAND ----------

df_result = df.filter("mail_domain = 'hotmail.com' OR sex = 'F'")
df_result.select("sex", "mail_domain").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### distinct and drop_duplicates

# COMMAND ----------

df_result = df.select("sex", "mail_domain").distinct()
df_result.display()

# COMMAND ----------

df_result = df.drop_duplicates(["sex"])
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF
# MAGIC 
# MAGIC User defined functions - It's a function created in python to be used inside Spark.
# MAGIC  - <img src="https://cdn-icons-png.flaticon.com/512/564/564619.png" alt="warn" width="25"/> Spark is developed in Scala (Java based), because of that it's run inside the JVM while Python run on Python compiler, when we need to process data with Python UDF it will need to exchange data between both and you can lose performance

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
import random

# COMMAND ----------

df_result = df.withColumn("expense", lit(random.random()*100))
df_result.select("expense").display()                 

# COMMAND ----------

def return_random_expense():
    return random.random()*100

# COMMAND ----------

print(return_random_expense())

# COMMAND ----------

spark_return_random_expense = udf(return_random_expense, FloatType())

df = df.withColumn("expense", spark_return_random_expense())
df.select("expense").display()                 

# COMMAND ----------

# MAGIC %md
# MAGIC ### groupBy

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

df_result = (
    df.groupBy("sex", "mail_domain")
    .agg(
        F.avg(col("expense")).alias("avg_expense"),
        F.sum(col("expense")).alias("sum_expense"),
        F.count(col("expense")).alias("count")
    )
)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### createOrReplaceTempView

# COMMAND ----------

df.createOrReplaceTempView("vw_df")

spark.sql("""
SELECT *
FROM vw_df
""").display()

# COMMAND ----------

df_agg = spark.sql("""
SELECT
    sex,
    sum(expense) AS sum_expense_by_sex
FROM vw_df
GROUP BY sex
""")

df_agg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins
# MAGIC 
# MAGIC 
# MAGIC [Joins types](https://spark.apache.org/docs/3.1.2/sql-ref-syntax-qry-select-join.html#join-types)
# MAGIC 
# MAGIC Must be one of:
# MAGIC  - inner *
# MAGIC  - cross
# MAGIC  - outer
# MAGIC  - full
# MAGIC  - fullouter
# MAGIC  - full_outer
# MAGIC  - left *
# MAGIC  - leftouter
# MAGIC  - left_outer
# MAGIC  - right *
# MAGIC  - rightouter
# MAGIC  - right_outer
# MAGIC  - [semi](https://spark.apache.org/docs/3.1.2/sql-ref-syntax-qry-select-join.html#semi-join)
# MAGIC  - leftsemi
# MAGIC  - left_semi
# MAGIC  - [anti](https://spark.apache.org/docs/3.1.2/sql-ref-syntax-qry-select-join.html#anti-join)
# MAGIC  - leftanti
# MAGIC  - left_anti
# MAGIC 
# MAGIC default inner
# MAGIC 
# MAGIC \* Most used

# COMMAND ----------

df_final = df.select("username", "sex", "expense")
df_final = df_final.join(df_agg, on="sex", how="left")
df_final.display()

# COMMAND ----------

df_final = (
    df_final
    .withColumn(
        "percent_expense_by_sex",
        (col("expense")/col("sum_expense_by_sex"))*100
    )
)

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Funtions sumary
# MAGIC 
# MAGIC Documentation of all functions used:
# MAGIC - [createDataFrame](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.SparkSession.createDataFrame.html)
# MAGIC - display: specific from Databricks workspace | spark core alternatives limit(10).[toPandas()](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.toPandas.html).head(10)
# MAGIC - [show](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.show.html)
# MAGIC - [printSchema](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.printSchema.html)
# MAGIC - [columns](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.columns.html)
# MAGIC - [select](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.select.html)
# MAGIC - [drop](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.select.html)
# MAGIC - [withColumnRenamed](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.withColumnRenamed.html)
# MAGIC - [col](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.col.html)
# MAGIC - [lit](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.lit.html)
# MAGIC - [withColumn](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html)
# MAGIC   - [functions](https://spark.apache.org/docs/3.2.1/api/python/reference/pyspark.sql.html#functions)
# MAGIC - [cast](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.Column.cast.html)
# MAGIC - [filter](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.filter.html)
# MAGIC - [distinct](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.distinct.html)
# MAGIC - [drop_duplicates](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.dropDuplicates.html)
# MAGIC - [udf](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.functions.udf.html)
# MAGIC - [groupBy](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.groupBy.html)
# MAGIC - [GroupedData functions](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.GroupedData.html#pyspark.sql.GroupedData)
# MAGIC - [createOrReplaceTempView](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html)
# MAGIC - [join](https://spark.apache.org/docs/3.2.1/api/python/reference/api/pyspark.sql.DataFrame.join.html)
