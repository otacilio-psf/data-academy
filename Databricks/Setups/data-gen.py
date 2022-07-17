# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

from uuid import uuid4
from faker import Faker
from random import random, randint
from datetime import date, timedelta
import numpy as np
import pandas as pd
import json
import time
import os

fake = Faker()

# COMMAND ----------

def generate_month_data(path="/FileStore/tmp", num_orders=100, year=None, month=None):
    if year is None:
        year=randint(2010,2020)
    
    if month is None:
        month=randint(1,12)
    
    print("Date:", month, "/", year)
    
    client_path = f"/dbfs{path}/client/{year}-{str(month).zfill(2)}"
    order_path = f"/dbfs{path}/orders/{year}-{str(month).zfill(2)}"
    os.makedirs(client_path, exist_ok=True)
    os.makedirs(order_path, exist_ok=True)
    
    fake_clients = [
        {
            "client_id": str(uuid4()),
            "client_name": fake.name(),
            "client_dob": fake.date_of_birth(minimum_age = 18, maximum_age = 65).strftime("%d/%m/%Y"),
            "client_country_code": fake.country_code("alpha-3")
            
        } for x in range((int(num_orders/5)))
    ]
    
    for j in fake_clients:
        j_client_id = j["client_id"]
        j_client_path = f"{client_path}/client-{j_client_id}.json"
        if not os.path.exists(j_client_path):
            with open(j_client_path, 'w') as f:
                json.dump(j, f)
    
    status_list = ["created","shipped","delivered"]
    
    fake_orders = [
        {
            "order_id": str(uuid4()),
            "client_id": np.random.choice([c["client_id"] for c in fake_clients]),
            "order_date": fake.date_between(start_date=date(year, month,1), end_date=date(year + int(month/12), month%12+1, 1) - timedelta(days=1)).strftime("%d/%m/%Y"),
            "order_status": np.random.choice(status_list, p=[0.10, 0.20, 0.70]),
            "order_total": round(random()*1000,2)
        } for x in range(num_orders)
    ]
    
    pd.DataFrame(fake_orders).to_csv(f"{order_path}/order-{int(time.time())}.csv", index=False)

# COMMAND ----------

dbutils.fs.rm("/FileStore/otacilio/data", True)

# COMMAND ----------

for i in range(1000):
    for m in range(1,13):
        generate_month_data("/FileStore/otacilio/data", year=2021, month=m)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/otacilio/data/client/2021-01"))
