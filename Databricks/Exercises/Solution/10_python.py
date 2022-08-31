# Databricks notebook source
# MAGIC %md
# MAGIC ## For every question `print` the resulted variable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 1.1 - Create a logic that will print `total_purchase`, if `total_purchase` is more then 500 it will recive 5% of discount
# MAGIC 
# MAGIC ## Question 1.2 ~ 1.3 - Applie the same logic as 1.1
# MAGIC 
# MAGIC 
# MAGIC - discount formula: total_purchase * (1 - 0.05)

# COMMAND ----------

#Answer 1.1
total_purchase = 100

if total_purchase > 500:
    print(total_purchase * (1 - 0.05))
else:
    print(total_purchase)

# COMMAND ----------

#Answer 1.2
total_purchase = 500

if total_purchase > 500:
    print(total_purchase * (1 - 0.05))
else:
    print(total_purchase)

# COMMAND ----------

#Answer 1.3
total_purchase = 501

if total_purchase > 500:
    print(total_purchase * (1 - 0.05))
else:
    print(total_purchase)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 2 - Print every element of the list `fruits` with upper case
# MAGIC 
# MAGIC - tip: upper() funtion

# COMMAND ----------

#Answer 2
fruits = ["apple", "banana", "cherry", "orange", "kiwi", "melon", "mango"]

for f in fruits:
    print(f.upper())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 3 - Create the function `calculate_final_price` that `return` the final price for a given `total_purchase`
# MAGIC 
# MAGIC - tip: same logic of Question 1.1

# COMMAND ----------

#Answer 3

def calculate_final_price(total_purchase):
    if total_purchase > 500:
        return total_purchase * (1 - 0.05)
    else:
        return total_purchase


# COMMAND ----------

assert calculate_final_price(100) == 100, "Final price should be 100"
assert calculate_final_price(500) == 500, "Final price should be 500"
assert calculate_final_price(510) == 484.5, "Final price should be 484.5"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 4 - Print the final price for each `total_purchase` in `order_dataset`

# COMMAND ----------

#Answer 4
order_dataset = [
    {"order_id": 1, "total_purchase": 105},
    {"order_id": 1, "total_purchase": 502},
    {"order_id": 1, "total_purchase": 692},
    {"order_id": 1, "total_purchase": 314},
    {"order_id": 1, "total_purchase": 420},
    {"order_id": 1, "total_purchase": 500}
]

for order in order_dataset:
    print(calculate_final_price(order["total_purchase"]))
