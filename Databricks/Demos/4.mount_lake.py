# Databricks notebook source
# MAGIC %md
# MAGIC Register Key Vauly
# MAGIC 
# MAGIC https:// databricks-instance #secrets/createScope

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = "0b72a4f8-9583-43d9-9e4d-6735ed200ca8"
scope = "kv-databricks"
key = "lake-secret"
tenant_id = "93f33571-550f-43cf-b09f-cd331338d086"
container_name = "data-lakehouse"
stg_account_name = "deepdivelake"

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope=scope,key=key),
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

dbutils.fs.mount(
  source = f"abfss://{container_name}@{stg_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/data-lakehouse")
