# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read\
    .schema(lap_times_schema)\
    .csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

lap_times_df.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename coloumns and add new columns
# MAGIC 1. RenamederiverId and Race ID
# MAGIC 2. add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed("raceId","race_Id")\
                      . withColumnRenamed("driverId", "driver_Id")\
                      .withColumn("Ingestion_Date", current_timestamp())\
                      .withColumn("v_data_source", lit(v_data_source))     

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

dbutils.notebook.exit("success")
