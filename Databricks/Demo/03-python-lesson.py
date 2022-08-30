# Databricks notebook source
# MAGIC %md
# MAGIC # import libs

# COMMAND ----------

import math
import json
from datetime import datetime
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## using math

# COMMAND ----------

def square_number(num):
    return num ** 2

print(square_number(2))
print(square_number(3))
print(square_number(4))

# COMMAND ----------

print(math.pow(2,2))
print(math.pow(3,2))
print(math.pow(4,2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## using json

# COMMAND ----------

my_dict = {
    "key1": 10,
    "key2": 20
}

print(type(my_dict))
print(my_dict)

my_json_string = json.dumps(my_dict)

print(type(my_json_string))
print(my_json_string)

my_json_string_2 = '{"key1": 30, "key2": 40}'
my_dict_2 = json.loads(my_json_string_2)

print(type(my_dict_2))
print(my_dict_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## using datatime

# COMMAND ----------

now = datetime.now() # current date and time
now

# COMMAND ----------

year = now.strftime("%Y")
print("year:", year)

month = now.strftime("%m")
print("month:", month)

day = now.strftime("%d")
print("day:", day)

time = now.strftime("%H:%M:%S")
print("time:", time)

date_time = now.strftime("%Y-%m-%dT%H:%M:%S")
print("date and time:",date_time)	


# COMMAND ----------

# MAGIC %md
# MAGIC ## using pandas

# COMMAND ----------

import pandas as pd
import numpy as np

k = 5
N = 10

alphabet = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')

#http://docs.scipy.org/doc/numpy/reference/generated/numpy.random.choice.html

df = pd.DataFrame({ 'A' : range(1, N + 1 ,1),
    'B' : np.random.randint(k, k + 100 , size=N),
    'C' : np.random.choice(np.array(alphabet, dtype="|S1"), N) })

df.head()
