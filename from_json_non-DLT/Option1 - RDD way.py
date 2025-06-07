# Databricks notebook source
from pyspark.sql.functions import from_json, col

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

input_df.count()

# COMMAND ----------

json_str = input_df.select('value').head()[0]
rdd = spark.sparkContext.parallelize([json_str])
schema = (
  spark.read.json(rdd)
  .schema.simpleString()
)
parsed_df = (input_df
  .withColumn('value',from_json(col('value'),schema))
  .select(
    'topic','partition','offset','timestamp','value.*'
  )
)
display(parsed_df.limit(10))

# COMMAND ----------

read_stream_df2 = (read_stream_df
  .withColumn('value', F.from_json(F.col('value'), one_row_json_schema))
  .select('topic', 'partition', 'offset', 'timestamp', 'value.*')
)
display(read_stream_df2)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

