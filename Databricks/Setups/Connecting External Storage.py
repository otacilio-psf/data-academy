# Databricks notebook source
# MAGIC %md
# MAGIC # Connecting External Storage
# MAGIC 
# MAGIC Options:
# MAGIC - Mount DataLake
# MAGIC - Set configuration notebook level
# MAGIC - Set configuration cluster level
# MAGIC   - Point directly to the path
# MAGIC 
# MAGIC References:
# MAGIC - [Secrets](https://docs.microsoft.com/pt-pt/azure/databricks/security/secrets/secret-scopes)
# MAGIC - [Mount DataLake](https://docs.databricks.com/data/mounts.html#mount-adls-gen2-or-blob-storage-with-abfs)
# MAGIC - [Direct access](https://docs.databricks.com/data/data-sources/azure/azure-storage.html)

# COMMAND ----------

# MAGIC %md ## Create kv scope
# MAGIC 
# MAGIC - Create kv-databricks scope
# MAGIC - Add sp-id
# MAGIC - Add sp-secret

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

container_name = ""
storage_account_name = ""
sp_client_id = dbutils.secrets.get(scope="kv-databricks", key="sp-id")
sp_client_secret = dbutils.secrets.get(scope="kv-databricks", key="sp-secret")
tenant_id = ""

# COMMAND ----------

# MAGIC %md ## Mount Datalake
# MAGIC 
# MAGIC Databricks no longer recommends because not compatible with the new feature *Unity Catalog*

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": sp_client_id,
          "fs.azure.account.oauth2.client.secret": sp_client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls(f"/mnt/{container_name}")

# COMMAND ----------

# MAGIC %md ## Direct access to Datalake - notebook configuration
# MAGIC Give the access rights at notebook run time

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", sp_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", sp_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

datalake_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
print(datalake_path)

# COMMAND ----------

dbutils.fs.ls(datalake_path)

# COMMAND ----------

# MAGIC %md ## Add path as secret
# MAGIC 
# MAGIC - Add lake-path

# COMMAND ----------

datalake_path_kv = dbutils.secrets.get(scope="kv-databricks",key="lake-path")

# COMMAND ----------

dbutils.fs.ls(datalake_path_kv)

# COMMAND ----------

# MAGIC %md ## Direct access to Datalake - cluster configuration
# MAGIC Give the access rights at cluster Advanced config

# COMMAND ----------

fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net OAuth
fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net sp_client_id
fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net {{secrets/<scope-name>/<secret-name>}}
fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net https://login.microsoftonline.com/{tenant_id}/oauth2/token

# COMMAND ----------

### ADD CONFIG AND RESTART CLUSTER

# COMMAND ----------

container_name = ""
storage_account_name = ""
datalake_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
dbutils.fs.ls(datalake_path)
