# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step - 1 Read the csv file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types  import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields =[StructField("raceId", IntegerType(),False),
                                      StructField("year", IntegerType(),True),
                                      StructField("round", IntegerType(), True),
                                      StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("date", DateType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("url", StringType(), True)
                                      ])

# COMMAND ----------

races_df = spark.read\
    .option("header",True)\
    .schema(races_schema)\
    .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -2 add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit,concat, to_timestamp

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp())\
                                   .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                    .withColumn("v_data_source", lit(v_data_source))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step -3 select only the columns required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'),col('round'),col('circuitId').alias('circuit_id'),\
                                                   col('name'),col('ingestion_date'),col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 4 write the output to the processed container in parquet format also partitioning
# MAGIC

# COMMAND ----------

races_final_df = races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl1038/processed/races

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

  dbutils.notebook.exit("success")
