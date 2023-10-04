# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount azure data lake containers for the project
# MAGIC ###Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 1. Set spark config with app/client Id, directory/tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount(list all mount, unmount)

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    client_id = dbutils.secrets.get(scope= 'formula1-scope', key = 'formula1-app-client-id')
    tenant_id= dbutils.secrets.get(scope= 'formula1-scope', key='formula1-app-tenant-id')
    client_secret = dbutils.secrets.get(scope='formula1-scope', key ='formula1-app-client-secret')

    # set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    # unmount the mount point if already exist
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storageaccount container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls('formula1dl1038','raw')

# COMMAND ----------

mount_adls('formula1dl1038','processed')

# COMMAND ----------

mount_adls('formula1dl1038','presentation')

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1dl1038/presentation")

# COMMAND ----------


