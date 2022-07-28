# Databricks notebook source
# MAGIC %md
# MAGIC ## Question 1.1 - Create four variables: country, city, country_population, this_year
# MAGIC ## Question 1.2 - Display all defined variables
# MAGIC ## Question 1.3 - Display the type of each variable

# COMMAND ----------

#Answer 1
country = "Portugal"
city = "Porto"
country_population = 10.31
this_year = 2022

print(country)
print(city)
print(country_population)
print(this_year)

print(type(country))
print(type(city))
print(type(country_population))
print(type(this_year))

# COMMAND ----------

# MAGIC %md ## Quesiton 2 - Calculator: Create all operations for num_1 and num_2
# MAGIC 
# MAGIC \+ Addition
# MAGIC 
# MAGIC \- Subtraction
# MAGIC 
# MAGIC \* Multiplication
# MAGIC 
# MAGIC / Division
# MAGIC 
# MAGIC % Modulus (remainder)

# COMMAND ----------

#Answer 2
num_1 = 5
num_2 = 4


print(num_1 + num_2) #Addition

print(num_1 - num_2) #Subtraction

print(num_1 * num_2) #Multiplication

print(num_1 / num_2) #Division

print(num_1 % num_2) #Modulus (remainder)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 3.1 - Find the size of the variable `short_news_part_1`
# MAGIC ## Question 3.2 - Change the value `100,000` to `85,000` on `short_news_part_2`
# MAGIC ## Question 3.3 - Concatenate `short_news_part_1` with `short_news_part_2` in a new variable `short_news`

# COMMAND ----------

short_news_part_1 = "Sardines will once again be the star of the show at the Portimão sardine festival, taking this place this year between 3 and 7 August"
print(short_news_part_1)

short_news_part_2 = "We hope to exceed 100,000 visitors, the number reached in the 2019 edition"
print(short_news_part_2)

# COMMAND ----------

#Answer 3.1
print(len(short_news_part_1))

# COMMAND ----------

#Answer 3.2
short_news_part_2 = short_news_part_2.replace('100,000', '85,000')
print(short_news_part_2)

# COMMAND ----------

#Answer 3.3
short_news = short_news_part_1 + ". " + short_news_part_2
print(short_news) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 4.1 - Create new variable `my_name` with your name
# MAGIC ## Question 4.2 - Modify the variable `message` in an away where we can format it with the variable `my_name`
# MAGIC ## Question 4.3 - `print()` the last word of `message`

# COMMAND ----------

#Answer 4.1
my_name = "Otacilio"
print(my_name)

# COMMAND ----------

#Answer 4.2
message = f"""
My name is {my_name} and I like to study python
"""

print(message)

# COMMAND ----------

#Answer 4.3
print(message[41:])
print(message[-7:])
