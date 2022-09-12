# Databricks notebook source
# MAGIC %pip install faker azure-storage-file-datalake

# COMMAND ----------

from azure.storage.filedatalake import DataLakeServiceClient
from os import listdir, makedirs
from faker import Faker
import pandas as pd
import json
import os

fake = Faker()

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
            

conn_str = dbutils.secrets.get(scope="kv-databricks", key="datalake-conn-str")
container = 'datalake'
adlfs = Datalake_handler(conn_str, container)

# COMMAND ----------

class ClassSetup():
    
    def __init__(self):
        user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        self._user_name = user_name.split("@")[0].replace(".","_").replace("-","_")
        self._user_path = f"sample/{self._user_name}"
        dbutils.fs.rm(f"file:/temp/{self._user_path}", True) 
        dbutils.fs.mkdirs(f"file:/temp/{self._user_path}/json") 
        dbutils.fs.rm(f"/mnt/academy_datalake/{self._user_path}", True)
    
    
    def create_csv(self):
        pd.DataFrame([fake.simple_profile() for _ in range(10000)]).to_csv(f"/temp/{self._user_path}/profile_csv.csv", index=False)
    
    def create_json(self):
        for i in range(1000):
            with open(f"/temp/{self._user_path}/json/profile_{str(i).zfill(3)}.json", 'w') as f:
                json.dump(fake.simple_profile(), f, default=str)
    
    def create_xlsm(self):
        pd.DataFrame([fake.simple_profile() for _ in range(10000)]).to_excel(f"/temp/{self._user_path}/profile_excel.xls", index=False)
    
    def create_all(self):
        self.create_csv()
        self.create_json()
        self.create_xlsm()
        adlfs.create_dir(self._user_path)
        adlfs.upload_dir(f"/temp/{self._user_path}", self._user_path)
        

# COMMAND ----------

ClassSetup().create_all()
