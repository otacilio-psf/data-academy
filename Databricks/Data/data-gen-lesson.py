# Databricks notebook source
# MAGIC %pip install faker azure-storage-file-datalake

# COMMAND ----------

class ClassSetup():
    
    def __init__(self):
        self._user_path = f"sample"
        dbutils.fs.rm(f"file:/temp/{self._user_path}", True) 
        dbutils.fs.mkdirs(f"file:/temp/{self._user_path}/json") 
        dbutils.fs.rm(f"/mnt/academy_datalake/{self._user_path}", True)
    
    
    def create_csv(self):
        population = [fake.simple_profile() for _ in range(10000)]
        for i in range(len(population)):
            population[i]['address'] = population[i]['address'].replace("\n"," ")
        pd.DataFrame(population).to_csv(f"/temp/{self._user_path}/profile_csv.csv", index=False)
    
    def create_json(self):
        for i in range(1000):
            j = fake.simple_profile()
            j['address'] = j['address'].replace("\n"," ")
            with open(f"/temp/{self._user_path}/json/profile_{str(i).zfill(3)}.json", 'w') as f:
                json.dump(j, f, default=str)
    
    def create_xlsm(self):
        population = [fake.simple_profile() for _ in range(10000)]
        for i in range(len(population)):
            population[i]['address'] = population[i]['address'].replace("\n"," ")
        pd.DataFrame(population).to_excel(f"/temp/{self._user_path}/profile_excel.xls", index=False)
    
    def create_all(self):
        self.create_csv()
        self.create_json()
        self.create_xlsm()
        adlfs.create_dir(self._user_path)
        adlfs.upload_dir(f"/temp/{self._user_path}", self._user_path)
        

# COMMAND ----------

ClassSetup().create_all()

# COMMAND ----------


