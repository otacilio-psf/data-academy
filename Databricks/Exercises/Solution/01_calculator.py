# Databricks notebook source
operation = dbutils.widgets.get("operation")
num1 = int(dbutils.widgets.get("num1"))
num2 = int(dbutils.widgets.get("num2"))

if operation == "-":
  result = f"{num1-num2}"
  dbutils.notebook.exit(result)
  
if operation == "+":
  result = f"{num1+num2}"
  dbutils.notebook.exit(result)

if operation == "*":
  result = f"{num1*num2}"
  dbutils.notebook.exit(result)
