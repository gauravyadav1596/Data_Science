# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

# COMMAND ----------

##Step - 1 Read the JSON file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
    .schema(constructors_schema)\
    .json("/mnt/formula1dl1038/raw/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####STEP - 2 Drop unwanted column from dataframe

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp, lit

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId","constructor_id")\
                                             .withColumnRenamed("onstructorRef","constructor_ref")\
                                             .withColumn("ingestion_date",current_timestamp())\
                                             .withColumn("v_data_source", lit(v_data_source))                         

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl1038/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


