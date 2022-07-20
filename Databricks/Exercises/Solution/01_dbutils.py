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
dbutils.fs.mkdirs('/FileStore/delta-data-academy/otacilio')

# COMMAND ----------

# Create file
dbutils.fs.put('/FileStore/delta-data-academy/otacilio/meu_file.txt', 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.')

# COMMAND ----------

# Read file
dbutils.fs.head('/FileStore/delta-data-academy/otacilio/meu_file.txt')

# COMMAND ----------

# Copy files
dbutils.fs.cp('/FileStore/delta-data-academy/data/client','/FileStore/delta-data-academy/otacilio/client', True)

# COMMAND ----------

# List files
display(dbutils.fs.ls('/FileStore/delta-data-academy/otacilio'))
display(dbutils.fs.ls('/FileStore/delta-data-academy/otacilio/client'))

# COMMAND ----------

# Remove folder
dbutils.fs.rm('/FileStore/delta-data-academy/otacilio', True)

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
dbutils.notebook.run('./01_calculator',  2000, {"operation":"-", "num1": 20, "num2": 10})

# COMMAND ----------

# Call 01_calculator for sum (+)
dbutils.notebook.run('./01_calculator',  2000, {"operation":"+", "num1": 20, "num2": 10})

# COMMAND ----------

# Call 01_calculator for multiplication (*)
dbutils.notebook.run('./01_calculator',  2000, {"operation":"*", "num1": 20, "num2": 10})
