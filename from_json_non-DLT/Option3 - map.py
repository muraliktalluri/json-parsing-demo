# Databricks notebook source
from pyspark.sql.functions import from_json, col,explode, first

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "dais-2025-json-parsing-demo", "prime-schema-from-json")

# Kafka topics
topic_name = 'from_json_test_topic'

# COMMAND ----------

input_df = (spark.read
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic_name)
  .option("startingOffsets", 'earliest' )
  .load())

input_df = input_df.selectExpr(
  'topic',
  'partition',
  'offset',
  'timestamp',
  'value::string as value'
)

# COMMAND ----------

parsed_df = (input_df
  .withColumn(
    'value',from_json(col('value'),'map<string,string>')
  )
  .select(
    'topic','partition','offset','timestamp',explode('value')
  )
  .groupBy(
    'topic','partition','offset','timestamp'
  )
  .pivot('key')
  .agg(first('value').cast('string'))            
)
display(parsed_df.limit(10))

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select schema_of_json('{"string_field": "example_9658", "int_field": 263, "double_field": 4.554618322882144, "array_field": [87, 38, 74, 7, 15], "dict_field": {"field1": 86, "field2": 4.143526422453212, "field3": "value_598"}, "nested_dict_field": {"field1": "nested_673", "field2": {"nested_field1": 174, "nested_field2": 5.924961429830619}}, "nested_array_field": [{"nested_field1": "nested_51", "nested_field2": 226, "nested_field3": 7.563096829477532, "nested_field4": [263, 299, 242, 211, 271]}, {"nested_field1": "nested_144", "nested_field2": 347, "nested_field3": 10.053779101755842, "nested_field4": [335, 306, 324, 345, 348]}]}')
# MAGIC

# COMMAND ----------

