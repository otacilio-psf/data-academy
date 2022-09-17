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

display(dbutils.fs.ls("/mnt/academy_datalake/sample"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV
# MAGIC CSV stand for comma-separated values, but the separator can be different:
# MAGIC   - ;
# MAGIC   - \t -> tab
# MAGIC   - | -> pipe

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("/mnt/academy_datalake/sample/profile_csv.csv")
df.display()

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", True)
    .load("/mnt/academy_datalake/sample/profile_csv.csv")
)
df.display()

# COMMAND ----------

schema = "username string, name string, sex int, addres string, mail string, birthdate date"

df = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .schema(schema)
    .load("/mnt/academy_datalake/sample/profile_csv.csv")
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

display(dbutils.fs.ls("/mnt/academy_datalake/sample/json"))

# COMMAND ----------

schema = (
    spark.read.format("json")
    .option("inferSchema", True)
    .load("/mnt/academy_datalake/sample/json/profile_000.json")
    .schema
)
print(schema)

# COMMAND ----------

df = (
    spark.read.format("json")
    .schema(schema)
    .load("/mnt/academy_datalake/sample/json")
)
df.display()

# COMMAND ----------

schema = "username string, name string, sex int, addres string, mail string, birthdate date"

df = (
    spark.read.format("json")
    .schema(schema)
    .load("/mnt/academy_datalake/sample/json")
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet
# MAGIC Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval. Is the best format for Spark!

# COMMAND ----------

df = (
    spark.read.format("parquet")
    .load("/mnt/academy_datalake/sample/profile_parquet.parquet")
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Excel spreedshet
# MAGIC It's not good or usual to have xls in our database but sometimes is inevitable, so how we can read it?
# MAGIC 
# MAGIC Spark core it's not compatible with excel files but since Spark 3.2 we have pandas API that allow us to do it

# COMMAND ----------

# MAGIC %pip install xlrd

# COMMAND ----------

import pyspark.pandas as pd

# COMMAND ----------

df = pd.read_excel("/mnt/academy_datalake/sample/profile_excel.xls").to_spark()

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Data - WIP

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data partitions - WIP

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV - WIP

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parquet - WIP

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations - WIP

# COMMAND ----------

# MAGIC %md
# MAGIC # Actions - WIP
