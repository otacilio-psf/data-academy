# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

print(dbutils.fs.ls("/FileStore"))

print(type(dbutils.fs.ls("dbfs:/FileStore")))

l = dbutils.fs.ls("dbfs:/FileStore")
print(l[0].path)

display(dbutils.fs.ls("file:/tmp"))
