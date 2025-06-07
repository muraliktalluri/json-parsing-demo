# Databricks notebook source
import pprint
import yaml
from pyspark.sql.types import StructType

kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "dais-2025-json-parsing-demo", "prime-schema-from-json")
topic_name = 'from_json_test_topic'
schema_volume_path = '/Volumes/murali_talluri/priming_schema/primed_schema_loc'

# COMMAND ----------

# Batch read from kafka for starting and ending offset to get sample of data

kafka_df = (spark.read.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("subscribe", topic_name)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", 'latest')
    .load()
  ) 

schema = list(kafka_df.selectExpr("schema_of_json_agg(value::string) as schema").limit(1).toPandas()['schema'])[0]
parsed_schema = StructType.fromDDL(schema)
valid_string_schema = ", ".join([ f"{field.name} {field.dataType.simpleString()}" for field in parsed_schema.fields])

primeSchemahint = {'primeSchemahint': valid_string_schema}

with open(f'{schema_volume_path}/schema.yaml', 'w') as file:
    yaml.dump(primeSchemahint, file)

# COMMAND ----------

with open(f'{schema_volume_path}/schema.yaml', 'r') as f:
	prime_schema = yaml.load(f, Loader=yaml.SafeLoader)
	
prime_schema['primeSchemahint']

# COMMAND ----------

