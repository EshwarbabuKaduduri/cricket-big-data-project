# Databricks notebook source
# DBTITLE 1,importing the required library
import requests
import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

# DBTITLE 1,create Catalog , Schema , Volumes
spark.sql("create catalog if not exists workspace")
spark.sql("create schema if not exists workspace.default")
spark.sql("create volume if not exists workspace.default.cricket_api_project")

base_path = "/Volumes/workspace.default.cricket_api_project"

# COMMAND ----------

# DBTITLE 1,calling cricket API
#Api_key = 'dd4dd816-1 fa4-4d7b-9476-06a18b6110721'
#Api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={Api_key}&offset=0"

Api_url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(Api_url)
response.raise_for_status()

api_data = response.json()

print(type(api_data))  # check type

# Safe handling
if isinstance(api_data, list):
    print(api_data[0].keys())
    print("eshwar")
else:
    print(api_data.keys())

print(json.dumps(api_data, indent=2)[:2000])


# COMMAND ----------

# DBTITLE 1,save raw API data in volumes
raw_path_base = f'/Volumes/workspace/default/cricket_api_project/raw.json'

with open (raw_path_base,'w')as file:
     json.dump(api_data,file)

print("the api data is saved at : ", raw_path_base)

# COMMAND ----------

# DBTITLE 1,Create bronze layer dataframe or table
bronze_data = [{
    "source_api":Api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]


bronze_schema = StructType([
    StructField("source_api",StringType(),True),
    StructField("raw_json",StringType(),True),
    StructField("ingestion_time",IntegerType(),True)
])



bronze_df = spark.createDataFrame(bronze_data,bronze_schema)\
    .withColumn("ingestion_time",current_timestamp())

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,save the bronze table
bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("workspace.default.cricket_api_project") 

print("Bronze table created successfully")


# COMMAND ----------


