# Databricks notebook source
# MAGIC %md
# MAGIC ## For every question `print` the resulted variable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 1.1 - Create a list with 3 Portuguese cities
# MAGIC ## Question 1.2 - Create a second list with 2 non-Portuguese cities

# COMMAND ----------

#Answer 1.1
portuguese_cities = ['Porto', 'Lisbon', 'Aveiro']
print(portuguese_cities)

# COMMAND ----------

#Answer 1.2
non_portuguese_cities = ['Paris', 'Madri']
print(non_portuguese_cities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 2.1 - `append` one more city on non portuguese cities list
# MAGIC ## Question 2.2 - Concatenate both list on `cities` list

# COMMAND ----------

#Answer 2.1
non_portuguese_cities.append('Amsterdam')
print(non_portuguese_cities)

# COMMAND ----------

#Answer 2.2
cities = portuguese_cities + non_portuguese_cities
print(cities)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 3.1 - Remove the second city from the list `cities`
# MAGIC ## Question 3.2 - Extract to the variable `last_city` the last city from the list `cities`

# COMMAND ----------

#Answer 3.1
del cities[1]
print(cities)

# COMMAND ----------

#Answer 3.2
last_city = cities.pop()
print(cities)
print(last_city)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 4.1 - Copy the list `cities` into `bkp_cities`
# MAGIC ## Question 4.2 - Delete all itens from `cities`

# COMMAND ----------

#Answer 4.1
bkp_cities = cities.copy()
print(bkp_cities)
print(cities)

# COMMAND ----------

#Answer 4.2
cities.clear()
print(bkp_cities)
print(cities)
