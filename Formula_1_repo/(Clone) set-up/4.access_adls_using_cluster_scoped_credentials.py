# Databricks notebook source
## Access Azure data lake using cluster scoped credentials
#1. Set files from demo container
#2.List files from demo container
#3. Read data from circuits.csv file 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl.dfs.core.windows.net"))

# COMMAND ----------


