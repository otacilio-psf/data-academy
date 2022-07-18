# Databricks notebook source
# MAGIC %md
# MAGIC Magic %Run

# COMMAND ----------

# MAGIC %run ./5.2.notebook_workflow

# COMMAND ----------

print(msg_52)

# COMMAND ----------

# MAGIC %md
# MAGIC Widgets

# COMMAND ----------

dbutils.widgets.text("foo", "fooDefault", "fooEmptyLabel")
print(dbutils.widgets.get("foo"))

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.notebook

# COMMAND ----------

dbutils.notebook.run("./5.3.notebook_workflow", 60,{"msg": "Hello from Notebook"})

# COMMAND ----------

print(msg)
