# Databricks notebook source
# MAGIC %pip install faker openpyxl

# COMMAND ----------

from faker import Faker

fake = Faker()

# COMMAND ----------

class ClassSetup():
    
    def __init__(self, path):
        self._user_path = f"{path}/sample"
        dbutils.fs.rm(self._user_path, True)
        dbutils.fs.rm("file:/sample_content", True)
        dbutils.fs.mkdirs("file:/sample_content")
        self._schema = "username string, name string, sex string, address string, mail string, birthdate date"
    
    
    def create_csv(self):
        population = [fake.simple_profile() for _ in range(10000)]
        for i in range(len(population)):
            population[i]['address'] = population[i]['address'].replace("\n"," ")
            
        (
            spark.createDataFrame(population, self._schema).coalesce(1)
            .write.format("csv").options(sep=";", header=True)
            .save(f"{self._user_path}/_temp_profile_csv")
        )
        csv_path = [
            i.path
            for i in dbutils.fs.ls(f"{self._user_path}/_temp_profile_csv")
            if ".csv" in i.path
        ][0]
        
        dbutils.fs.mv(csv_path, f"{self._user_path}/profile_csv.csv")
        dbutils.fs.rm(f"{self._user_path}/_temp_profile_csv", True)
    
    def create_json(self):
        population = [fake.simple_profile() for _ in range(10000)]
        for i in range(len(population)):
            population[i]['address'] = population[i]['address'].replace("\n"," ")
            
        (
            spark.createDataFrame(population, self._schema).repartition(1000)
            .write.json(f"{self._user_path}/profile_json")
        )
                
        not_json_path = [
            i.path
            for i in dbutils.fs.ls(f"{self._user_path}/profile_json")
            if ".json" not in i.path
        ]
        for njp in not_json_path:
            dbutils.fs.rm(njp)
            
        dbutils.fs.mv(
            dbutils.fs.ls(f"{self._user_path}/profile_json")[0].path,
            f"{self._user_path}/profile_json/profile_000.json"
        )
    
    def create_xlsx(self):
        population = [fake.simple_profile() for _ in range(10000)]
        for i in range(len(population)):
            population[i]['address'] = population[i]['address'].replace("\n"," ")
            
        (
            spark.createDataFrame(population, self._schema)
            .to_pandas_on_spark()
            .to_excel("/sample_content/profile_excel.xlsx", index=False)
        )
        dbutils.fs.mv("file:/sample_content/profile_excel.xlsx", f"{self._user_path}/profile_excel.xlsx")
    
    def create_parquet(self):
        population = [fake.simple_profile() for _ in range(10000)]
        for i in range(len(population)):
            population[i]['address'] = population[i]['address'].replace("\n"," ")
            
        (
            spark.createDataFrame(population, self._schema).coalesce(1)
            .write.parquet(f"{self._user_path}/_temp_profile_parquet")
        )
        parquet_path = [
            i.path
            for i in dbutils.fs.ls(f"{self._user_path}/_temp_profile_parquet")
            if ".parquet" in i.path
        ][0]
        
        dbutils.fs.mv(parquet_path, f"{self._user_path}/profile_parquet.parquet")
        dbutils.fs.rm(f"{self._user_path}/_temp_profile_parquet", True)

    def create_delta(self):
        for _ in range(5):
            population = [fake.simple_profile() for _ in range(2000)]
            for i in range(len(population)):
                population[i]['address'] = population[i]['address'].replace("\n"," ")

            (
                spark.createDataFrame(population, self._schema).coalesce(100)
                .write.format("delta").mode("append")
                .save(f"{self._user_path}/profile_delta")
            )
    
    def create_all(self):
        self.create_csv()
        self.create_json()
        self.create_xlsx()
        self.create_parquet()
        self.create_delta()
        

# COMMAND ----------

ClassSetup("/mnt/datalake").create_all()
