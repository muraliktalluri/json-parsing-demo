# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

volume_path = '/Volumes/murali_talluri/priming_schema/variant'
data_path = f'{volume_path}/data'

# COMMAND ----------

input_df = spark.table('murali_talluri.priming_schema.from_json_test_schema1')
required_fileds = ['array_field','dict_field','double_field','int_field','nested_array_field','nested_dict_field','string_field']
input_df = input_df.select(required_fileds)

# COMMAND ----------

for schema_suffix in range(1, 101):
  print(f'processing {schema_suffix}')
  current_df = input_df.withColumn('type', F.lit(schema_suffix))
  for field in required_fileds:
    current_df = current_df.withColumn(f'{field}_{schema_suffix}', F.col(field)).drop(field)
  current_df.repartition(10).write.mode('append').format('json').save(data_path)