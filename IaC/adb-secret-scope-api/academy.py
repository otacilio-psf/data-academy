from secret_handler_databricks import DatabricksSecret
from dotenv import load_dotenv
import os

load_dotenv()


dbs = DatabricksSecret(os.getenv("DATABRICKS_HOST"), os.getenv("DATABRICKS_TOKEN"))

dbs.list_scope()

dbs.create_scope("poc-cloud")

dbs.create_secret("poc-cloud", "sp-secret", os.getenv("SP_SECRET"))

dbs.list_secret("poc-cloud")