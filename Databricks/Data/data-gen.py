# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from uuid import uuid4
from faker import Faker
from random import random, randint
from datetime import datetime, date, timedelta
from pyspark.sql.functions import col
import numpy as np
import json
import time

fake = Faker()

# COMMAND ----------

def generate_month_data(path, num_orders=100, year=None, month=None):
    if year is None:
        year=randint(2010,2020)
    
    if month is None:
        month=randint(1,12)
    
    print(f"Date: {month}/{year}")
    
    # Create fake clients
    fake_clients = [
        {
            "client_id": str(uuid4()),
            "client_name": fake.name(),
            "client_dob": fake.date_of_birth(minimum_age = 18, maximum_age = 65).strftime("%d/%m/%Y"),
            "client_country_code": fake.country_code("alpha-3"),
            "client_start_datetime": datetime(year, month,1).isoformat()
            
        } for x in range((int(num_orders/5)))
    ]
    
    # Write fake clients json
    fake_clients_schema = "client_id string, client_name string, client_dob string, client_country_code string, client_start_datetime string"
    
    (
        spark.createDataFrame(fake_clients, fake_clients_schema)
        .write.mode("append").format("json")
        .save(f"{path}/clients")
    )
    
    # Clean up fake clients folder
    not_json_path = [
        i.path
        for i in dbutils.fs.ls(f"{path}/clients")
        if ".json" not in i.path
    ]
    for njp in not_json_path:
        dbutils.fs.rm(njp)
    
    # Create Fake Orders
    status_list = ["created","shipped","delivered"]
    fake_client_id = [c["client_id"] for c in fake_clients]
    
    fake_orders = [
        {
            "order_id": str(uuid4()),
            "client_id": str(np.random.choice(fake_client_id)),
            "order_date": fake.date_between(start_date=date(year, month,1), end_date=date(year + int(month/12), month%12+1, 1) - timedelta(days=1)).strftime("%d/%m/%Y"),
            "order_status": str(np.random.choice(status_list, p=[0.10, 0.20, 0.70])),
            "order_total": random()*1000,
        } for x in range(num_orders)
    ]

    fake_orders_schema = "order_id string, client_id string, order_date string, order_status string, order_total float"
    
    # Write Fake orders CSV
    (
        spark.createDataFrame(fake_orders, fake_orders_schema)
        .withColumn("order_total", col("order_total").cast("decimal(10,2)"))
        .write.mode("append").format("csv").option("header", "true")
        .save(f"{path}/orders")
    )
    
    # Clean up CSV
    not_csv_path = [
        i.path
        for i in dbutils.fs.ls(f"{path}/orders")
        if ".csv" not in i.path
    ]
    for ncp in not_csv_path:
        dbutils.fs.rm(ncp)

# COMMAND ----------

dbutils.fs.rm("/mnt/datalake/raw/clients", True)
dbutils.fs.rm("/mnt/datalake/raw/orders", True)

# COMMAND ----------

for y in [2018,2019,2020,2021]:
    for m in range(1,13):
        for i in range(10):
            generate_month_data("/mnt/datalake/raw", num_orders=randint(700,1100), year=y, month=m)
