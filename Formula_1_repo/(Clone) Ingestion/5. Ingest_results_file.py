# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest results.json file

# COMMAND ----------

### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

 from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType ( fields =[StructField("resultId", IntegerType(),False),
                                      StructField("raceId", IntegerType(),True),
                                      StructField("driverId", IntegerType(),True),
                                      StructField("constructorId", IntegerType(),True),
                                      StructField("number", IntegerType(),True),
                                      StructField("grid", IntegerType(),True),
                                      StructField("position", IntegerType(),True),
                                      StructField("positionText", StringType(),True),
                                      StructField("positionOrder", IntegerType(),True),
                                      StructField("points", FloatType(),True),
                                      StructField("laps", IntegerType(),True),
                                      StructField("time", StringType(),True),
                                      StructField("milliseconds", IntegerType(),True),
                                      StructField("fastestLap", IntegerType(),True),
                                      StructField("rank", IntegerType(),True),
                                      StructField("fastestLaptime", StringType(),True),
                                      StructField("fastestLapSpeed", FloatType(),True),
                                      StructField("statusId", StringType(),True)])

# COMMAND ----------

 results_df=spark.read \
     .schema(results_schema)\
    .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and addd new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_Id")\
                                    .withColumnRenamed("raceId", "race_Id")\
                                    .withColumnRenamed("driverId", "driver_Id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumnRenamed("positionText", "position_text")\
                                    .withColumnRenamed("positionOrder", "position_order")\
                                    .withColumnRenamed("fastestlap", "fastest_lap")\
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                    .withColumn("ingestion_date", current_timestamp())\
                                    .withColumn("v_data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - write the output to processed container in parquet format

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_Id').parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------


