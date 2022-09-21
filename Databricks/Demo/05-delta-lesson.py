# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake
# MAGIC 
# MAGIC <img src="https://delta.io/static/delta-hp-hero-bottom-46084c40468376aaecdedc066291e2d8.png" alt="delta"/>

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading and writing Delta Lake

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/datalake/sample/profile_delta")
df.display()

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("/FileStore/otacilio/spark-lesson/delta_1")

# COMMAND ----------

df = spark.read.format("delta").load("/FileStore/otacilio/spark-lesson/delta_1")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Key Features

# COMMAND ----------

# MAGIC %md
# MAGIC ## ACID Transactions
# MAGIC 
# MAGIC Protect your data with serializability, the strongest level of isolation
# MAGIC 
# MAGIC **A**tomicity
# MAGIC - Guarantees that each transaction is treated as a single "unit", which either succeeds completely or fails completely
# MAGIC **C**onsistency (Correctness)
# MAGIC - Ensures that a transaction can only bring the database from one valid state to another
# MAGIC **I**solation
# MAGIC - Ensures that concurrent execution of transactions leaves the database in the same state that would have been obtained if the transactions were executed sequentially
# MAGIC **D**urability
# MAGIC - Guarantees that once a transaction has been committed, it will remain committed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Open Source
# MAGIC 
# MAGIC Community driven, open standards, open protocol, open discussions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unified Batch/Streaming
# MAGIC 
# MAGIC Exactly once semantics ingestion to backfill to interactive queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## DML Operations
# MAGIC 
# MAGIC SQL, Scala/Java and Python APIs to merge, update and delete datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge

# COMMAND ----------

df_update = spark.sql("""
SELECT username, name, 'N' AS sex, address, mail, birthdate
FROM delta.`/mnt/datalake/sample/profile_delta`
LIMIT 100
""")

df_insert = spark.read.format("parquet").load("/mnt/datalake/sample/profile_parquet.parquet")

df_merge = df_update.union(df_insert)

df_merge.display()

df_merge.createOrReplaceTempView("vw_merge")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta.`/mnt/datalake/sample/profile_delta` AS target
# MAGIC USING vw_merge AS source
# MAGIC 
# MAGIC ON target.username = source.username AND target.mail = source.mail
# MAGIC 
# MAGIC WHEN MATCHED
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(sex)
# MAGIC FROM delta.`/mnt/datalake/sample/profile_delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/mnt/datalake/sample/profile_delta`
# MAGIC WHERE mail LIKE '%hotmail%'

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/mnt/datalake/sample/profile_delta`
# MAGIC WHERE mail LIKE '%hotmail%'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta.`/mnt/datalake/sample/profile_delta`
# MAGIC WHERE mail LIKE '%hotmail%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/mnt/datalake/sample/profile_delta`
# MAGIC      SET sex = 'Male'
# MAGIC WHERE sex = 'M'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(sex)
# MAGIC FROM delta.`/mnt/datalake/sample/profile_delta`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit History
# MAGIC 
# MAGIC Delta Lake log all change details providing a fill audit trail

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY delta.`/mnt/datalake/sample/profile_delta`
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Travel
# MAGIC 
# MAGIC Access/revert to earlier versions of data for audits, rollbacks, or reproduce

# COMMAND ----------

(
    spark.read
    .format("delta")
    .load("/mnt/datalake/sample/profile_delta")
    .count()
)

# COMMAND ----------

(
    spark.read
    .format("delta")
    .option("timestampAsOf", "2022-09-21T21:49:06.000+0000")
    .load("/mnt/datalake/sample/profile_delta")
    .count()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM delta.`/mnt/datalake/sample/profile_delta` VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimizations
# MAGIC Delta Lake provides optimizations that accelerate data lake operations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compaction (bin-packing)
# MAGIC 
# MAGIC Delta Lake can improve the speed of read queries from a table by coalescing small files into larger ones (default ~200mb)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake/sample/profile_delta"))

# COMMAND ----------

spark.sql("""
OPTIMIZE delta.`/mnt/datalake/sample/profile_delta`
""").display()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake/sample/profile_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vacuum
# MAGIC 
# MAGIC You can remove files no longer referenced by a Delta table and are older than the retention threshold by running the vacuum command on the table. vacuum is not triggered automatically. The default retention threshold for the files is 7 days.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta.`/mnt/datalake/sample/profile_delta` RETAIN 0 HOURS

# COMMAND ----------

display(dbutils.fs.ls("/mnt/datalake/sample/profile_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data skipping
# MAGIC 
# MAGIC Data skipping information is collected automatically when you write data into a Delta Lake table. Delta Lake takes advantage of this information (minimum and maximum values for each column) at query time to provide faster queries.
# MAGIC 
# MAGIC A bad partition strategie of the folder can decrese performance of this feature

# COMMAND ----------

# MAGIC %md
# MAGIC ## Z-Ordering (multi-dimensional clustering)
# MAGIC 
# MAGIC Z-Ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake in data-skipping algorithms. This behavior dramatically reduces the amount of data that Delta Lake on Apache Spark needs to read

# COMMAND ----------

spark.sql("""
OPTIMIZE delta.`/mnt/datalake/sample/profile_delta` ZORDER BY sex
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Checking history

# COMMAND ----------

spark.sql("""
DESCRIBE HISTORY delta.`/mnt/datalake/sample/profile_delta`
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta documentation
# MAGIC 
# MAGIC - [Delta lake site](https://delta.io/)
# MAGIC - [Docs](https://docs.delta.io/2.0.0/index.html)
# MAGIC - [Table utility commands](https://docs.delta.io/2.0.0/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table)
# MAGIC - [DML operations](https://docs.delta.io/2.0.0/delta-update.html)
# MAGIC - [Optimizations](https://docs.delta.io/2.0.0/optimizations-oss.html)
