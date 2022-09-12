# Databricks notebook source
# MAGIC %pip install faker azure-storage-file-datalake

# COMMAND ----------

from uuid import uuid4
from faker import Faker
from random import random, randint
from datetime import datetime, date, timedelta
import numpy as np
import pandas as pd
import json
import time
import os

fake = Faker()

# COMMAND ----------

def generate_month_data(path, num_orders=100, year=None, month=None):
    if year is None:
        year=randint(2010,2020)
    
    if month is None:
        month=randint(1,12)
    
    print("Date:", month, "/", year)
    
    client_path = f"{path}/client"
    order_path = f"{path}/orders/year_month={year}-{str(month).zfill(2)}"
    os.makedirs(client_path, exist_ok=True)
    os.makedirs(order_path, exist_ok=True)
    
    fake_clients = [
        {
            "client_id": str(uuid4()),
            "client_name": fake.name(),
            "client_dob": fake.date_of_birth(minimum_age = 18, maximum_age = 65).strftime("%d/%m/%Y"),
            "client_country_code": fake.country_code("alpha-3"),
            "client_start_datetime": datetime(year, month,1).isoformat()
            
        } for x in range((int(num_orders/5)))
    ]
    
    for j in fake_clients:
        j_client_id = j["client_id"]
        j_client_path = f"{client_path}/client-{j_client_id}.json"
        if not os.path.exists(j_client_path):
            with open(j_client_path, 'w') as f:
                json.dump(j, f)
    
    status_list = ["created","shipped","delivered"]
    fake_client_id = [c["client_id"] for c in fake_clients]
    
    fake_orders = [
        {
            "order_id": str(uuid4()),
            "client_id": np.random.choice(fake_client_id),
            "order_date": fake.date_between(start_date=date(year, month,1), end_date=date(year + int(month/12), month%12+1, 1) - timedelta(days=1)).strftime("%d/%m/%Y"),
            "order_status": np.random.choice(status_list, p=[0.10, 0.20, 0.70]),
            "order_total": round(random()*1000,2)
        } for x in range(num_orders)
    ]
    
    pd.DataFrame(fake_orders).to_csv(f"{order_path}/order-{int(time.time())}.csv", index=False)


# COMMAND ----------

dbutils.fs.rm("/FileStore/delta-data-academy/data", True)
dbutils.fs.rm("file:/temp/data",True)

# COMMAND ----------

for y in [2018,2019,2020,2021]:
    for m in range(1,13):
        for i in range(10):
            generate_month_data("/temp/data", num_orders=randint(700,1100), year=y, month=m)

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from os import listdir, makedirs

class Datalake_handler():
    
    def __init__(self, conn_str, container):
        self._adls = DataLakeServiceClient.from_connection_string(conn_str)
        self._fs = self._adls.get_file_system_client(file_system=container)
        
        self._container = container

    def create_dir(self, dir_path):
        try:
            self._fs.create_directory(dir_path)
        except Exception as e:
            print(e)
   
    def upload_dir(self, local_dir_path, remote_dir_path, dir_scope=False):
        for root, _, files in os.walk(local_dir_path):
            for name in files:
                local_file_path = os.path.join(root, name)
                file_name = local_file_path.replace(local_dir_path + os.sep,'')
                self.upload_file(local_file_path,
                                 f'{remote_dir_path}/{file_name}',
                                 dir_scope)
                
    def upload_file(self, local_file_path, remote_file_path, dir_scope=False):
        if dir_scope: scope = self._dir
        else: scope = self._fs
        try: 
            file_client = scope.get_file_client(remote_file_path)
            with open(local_file_path,'r') as local_file:
                file_contents = local_file.read()
                file_client.upload_data(file_contents, overwrite=True)
        except Exception as e:
            print(e)

# COMMAND ----------

conn_str = dbutils.secrets.get(scope="kv-databricks", key="datalake-conn-str")
container = 'datalake'
adlfs = Datalake_handler(conn_str, container)

adlfs.create_dir('raw')

# COMMAND ----------

adlfs.upload_dir("/temp/data", 'raw')

# COMMAND ----------

dbutils.fs.ls("/mnt/academy_datalake/raw/")

# COMMAND ----------

df_orders = spark.read.format("csv").options(header=True).load("/mnt/academy_datalake/raw/orders/")
df_orders.display()
print(df_orders.count())

# COMMAND ----------

dbutils.fs.ls("/mnt/academy_datalake/raw/client/")

# COMMAND ----------

########## test

# COMMAND ----------

schema = spark.read.format("json").load("/FileStore/otacilio/data/client/year_month=2021-01/client-0e22be7f-8225-46e0-bbd8-8a045a131912.json").schema
df_client = spark.read.format("json").schema(schema).load("/FileStore/otacilio/data/client")
df_client.display()
print(df_client.count())

# COMMAND ----------

df_join = df_orders.join(df_client.drop("year_month"), 'client_id')
df_join.display()
print(df_join.count())

# COMMAND ----------

import pyspark.sql.functions as F

(
    df_join
    .groupBy("client_country_code")
    .agg(F.sum("order_total").alias("country_total"))
).display()
