# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount azure data lake using service principle
# MAGIC ###Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 1. Set spark config with app/client Id, directory/tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount(list all mount, unmount)

# COMMAND ----------



# COMMAND ----------

client_id = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-app-client-id')
tenant_id= dbutils.secrets.get(scope= 'formula1-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key ='formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dl1038.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dl/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dl/demo')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


