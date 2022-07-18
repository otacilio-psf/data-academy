# Databricks notebook source
msg = dbutils.widgets.get("msg")

# COMMAND ----------

if msg == "Hello from Notebook":
    output_msg = "Hi Databricks Notebook"
elif msg == "Hello from ADF":
    output_msg = "Hi Data Factory"
else:
    name = msg.replace("Hello from ", "")
    output_msg = f"Hello, {name}"

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "output": output_msg
}))
