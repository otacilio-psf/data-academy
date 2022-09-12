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

df = spark.createDataFrame([('Otacilio', 1), ('Pedro', 6), ('Andre', 4), ('Maria', 7)], schema = ['name', 'id'])
df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType

schema = StructType([StructField('name', StringType()), StructField('id', IntegerType())])
