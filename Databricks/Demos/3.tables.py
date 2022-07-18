# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE users
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )

# COMMAND ----------

from pyspark.sql import Row
data = [
  Row(id=1, name="Karol"),
  Row(id=2, name="Pawel"),
  Row(id=3, name="Piotr"),
  Row(id=4, name="Rafal")
]

df = spark.createDataFrame(data)
df.createOrReplaceTempView("vw_update_data")

# COMMAND ----------

spark.sql("""
MERGE INTO users AS t
USING vw_update_data AS v
ON t.id = v.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
""").display()

# COMMAND ----------

spark.table("users").display()

# COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse/users")

# COMMAND ----------

# MAGIC %md
# MAGIC For tables defined in the metastore, you can optionally specify the LOCATION as a path. Tables created with a specified LOCATION are considered unmanaged by the metastore. Unlike a managed table, where no path is specified, an unmanaged table’s files are not deleted when you DROP the table.

# COMMAND ----------

spark.sql("""
CREATE TABLE users2
(
  id INT,
  name STRING
)
USING DELTA
LOCATION '/delta/users2'
""").display()

# COMMAND ----------

spark.sql("""
MERGE INTO users2 AS t
USING vw_update_data AS v
ON t.id = v.id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *
""").display()

# COMMAND ----------

dbutils.fs.ls("/delta/users2")

# COMMAND ----------

# MAGIC %md
# MAGIC Modification history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY users2

# COMMAND ----------

# MAGIC %md
# MAGIC DROP TABLE

# COMMAND ----------

spark.sql("""
DROP TABLE users
""").display()

# COMMAND ----------

spark.sql("""
DROP TABLE users2
""").display()

# COMMAND ----------

dbutils.fs.ls("/user/hive/warehouse/users")

# COMMAND ----------

dbutils.fs.ls("/delta/users2")

# COMMAND ----------

dbutils.fs.rm("/delta/users2", True)
