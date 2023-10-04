# Databricks notebook source
# MAGIC %md
# MAGIC #### Read all the data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

driver_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location","circuit_location")


# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")\
    .withColumnRenamed("name","race_name")\
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed("time","race_time")

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id)\
                            .join(driver_df, results_df.driver_id == driver_df.driver_id)\
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)    

# COMMAND ----------


