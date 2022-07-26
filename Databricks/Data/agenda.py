# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from uuid import uuid4
from faker import Faker

fake = Faker()

# COMMAND ----------

def generate_agenda(num_persons=100):
    
    fake_agenda = [
        {
            "id": str(uuid4()),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "phone_number": fake.msisdn()
            
        } for x in range(num_persons)
    ]
    
    return fake_agenda


# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS data_sample")
spark.sql(f"DROP TABLE IF EXISTS data_sample.agenda")
spark.createDataFrame(generate_agenda(800)).select("id", "first_name", "last_name", "phone_number").write.mode("overwrite").saveAsTable("data_sample.agenda")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_sample.agenda
