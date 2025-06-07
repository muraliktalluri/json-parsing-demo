# Databricks notebook source
from pyspark.sql.functions import from_json, col,schema_of_json

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

json_str = input_df.select('value').head()[0]
df = spark.createDataFrame([(json_str,)],["json_col"])
schema = (
  df.select(schema_of_json(col("json_col")))
  .collect()[0][0]
)
parsed_df = (input_df
  .withColumn('value',from_json(col('value'),schema))
  .select(
    'topic','partition','offset','timestamp','value.*'
  )
)
display(parsed_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **SQL way of schema_of_json**

# COMMAND ----------

read_stream_df.limit(1).createOrReplaceTempView('read_stream_df')
one_row_json_schema2 = spark.sql('select schema_of_json(value) from read_stream_df').head()[0]
read_stream_df3 = (read_stream_df
  .withColumn('value', F.from_json(F.col('value'), one_row_json_schema2))
  .select('topic', 'partition', 'offset', 'timestamp', 'value.*')
)
display(read_stream_df3)

# COMMAND ----------

