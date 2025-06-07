# Databricks notebook source
import dlt
from pyspark.sql.functions import col, from_json

# COMMAND ----------

kafka_bootstrap_servers = dbutils.secrets.get( "dais-2025-json-parsing-demo", "prime-schema-from-json")
topic_name = 'from_json_test_topic'

# COMMAND ----------

@dlt.table(name=topic_name + '_no_schema')
def read_from_kafka():
  return (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", topic_name)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger",10000000)
    .load()
    .withColumn(
      "value_parsed", 
      from_json(
        col("value").cast('string'), 
        None, 
        {"schemaLocationKey": topic_name}
      )
    )
    .selectExpr(
      'topic',
      'partition',
      'offset',
      'timestamp',
      'value_parsed.*'
    )
  )  