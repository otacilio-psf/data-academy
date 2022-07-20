# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# MAGIC %md ## Using dbutils.fs
# MAGIC 
# MAGIC - Create your personal folder inside '/FileStore/delta-data-academy/**studant-name**'
# MAGIC - Use put to create a file 'meu_file.txt' inside your folder with the content 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.'
# MAGIC - Read the beginning of the file 'meu_file.txt' with head
# MAGIC - Copy the folder '/FileStore/delta-data-academy/data/client' to your own client folder
# MAGIC - List the content of your folder
# MAGIC - Remove your folder

# COMMAND ----------

# Create folder


# COMMAND ----------

# Create file


# COMMAND ----------

# Read file


# COMMAND ----------

# Copy files


# COMMAND ----------

# List files


# COMMAND ----------

# Remove folder


# COMMAND ----------

# MAGIC %md ## Using dbutils.notebook and dbutils.widgets
# MAGIC 
# MAGIC - Create a new notebook named '01_calculator' that recive parameters with the follow code
# MAGIC 
# MAGIC ```
# MAGIC operation = <IMPLEMENT>
# MAGIC num1 = int(<IMPLEMENT>)
# MAGIC num2 = int(<IMPLEMENT>)
# MAGIC 
# MAGIC if operation == "-":
# MAGIC   result = f"{num1-num2}"
# MAGIC   <IMPLEMENT>(result)
# MAGIC if operation == "+":
# MAGIC   result = f"{num1+num2}"
# MAGIC   <IMPLEMENT>(result)
# MAGIC if operation == "*":
# MAGIC   result = f"{num1*num2}"
# MAGIC   <IMPLEMENT>(result)
# MAGIC ```
# MAGIC 
# MAGIC - Call calculator passing parameters of operation and two numbers

# COMMAND ----------

# Call 01_calculator for subtraction (-)


# COMMAND ----------

# Call 01_calculator for sum (+)


# COMMAND ----------

# Call 01_calculator for multiplication (*)

