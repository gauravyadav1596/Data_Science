# Databricks notebook source
# MAGIC %md
# MAGIC #### Access azure data lake using service principle
# MAGIC ###Steps to follow
# MAGIC 1. Register Azure AD Application / Service principle
# MAGIC 1. generate a secret/ password for the application
# MAGIC 1. set spark config with app/ client ID, Directory/ tenant ID & Secret
# MAGIC 1. Assign Role 'Storage Blob Data contributor' to data lake.

# COMMAND ----------



# COMMAND ----------

client_id = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-app-client-id')
tenant_id= dbutils.secrets.get(scope= 'formula1-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key ='formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl1038.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl1038.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl1038.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl1038.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl1038.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl1038.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl1038.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


