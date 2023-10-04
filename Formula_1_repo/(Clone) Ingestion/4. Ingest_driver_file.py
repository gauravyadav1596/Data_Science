# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using the Spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id
# MAGIC 2. driverref renamed to driver_ref
# MAGIC 3. Ingestion date added
# MAGIC 4. name added with concatenation of forename and surname
# MAGIC

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(),False),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId",IntegerType(),False),
                                    StructField("driverRef",StringType(),True),
                                    StructField("number",IntegerType(),True),
                                    StructField("code",StringType(),True),
                                    StructField("name",name_schema),
                                    StructField("dob",DateType(),True),
                                    StructField("nationality",StringType(),True),
                                    StructField("url",StringType(),True)
                                    ])

# COMMAND ----------

drivers_df = spark.read\
    .schema(drivers_schema)\
    .json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename columns and add new column
# MAGIC 1. driverId renamed  to driver_Id
# MAGIC 2. driverRef to driver_Ref
# MAGIC 3. ingesting date added
# MAGIC 4. name added with concatenation of forname and surname

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, lit, concat,current_timestamp

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_Id")\
                                    .withColumnRenamed("driverRef", "driver_Ref")\
                                    .withColumn("Ingestion_date", current_timestamp())\
                                    .withColumn("name", concat(col("name.forename"),lit(' '), col("name.surname"))) \
                                    .withColumn("v_data_source", lit(v_data_source))                                           

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop unwanted column
# MAGIC 1. name.forename
# MAGIC 2. name.surname
# MAGIC 3. url

# COMMAND ----------

driver_final_df = drivers_with_columns_df.drop("url")

# COMMAND ----------

driver_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

dbutils.notebook.exit("success")
