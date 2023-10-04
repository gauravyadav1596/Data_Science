# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the spark dataframe reader API

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

qualifying_schema = StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("q1",StringType(),True),
                                     StructField("q2",StringType(),True),
                                     StructField("q3",StringType(),True)
                                     ])

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option("multiline", True)\
    .json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename coloumns and add new columns
# MAGIC 1. RenamederiverId and Race ID
# MAGIC 2. add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , lit

# COMMAND ----------

final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_Id")\
                        .withColumnRenamed("driverId", "driver_Id")\
                        .withColumnRenamed("raceId","race_Id")\
                        .withColumnRenamed("constructorId", "constructor_Id")\
                        .withColumn("Ingestion_Date", current_timestamp())\
                        .withColumn("v_data_source", lit(v_data_source))   

# COMMAND ----------

# MAGIC %md 
# MAGIC #### step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

dbutils.notebook.exit("success")
