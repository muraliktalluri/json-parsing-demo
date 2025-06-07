# Databricks notebook source
volume_path = '/Volumes/murali_talluri/priming_schema/variant'
data_path = f'{volume_path}/data'
stream_path = f'{volume_path}/stream/'
checkpoint_path = f"{stream_path}/checkpoint"
table_name = 'murali_talluri.priming_schema.bronze'

# COMMAND ----------

stream_df = (
  spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "JSON")
  .option("singleVariantColumn", "data")
  .load(f'{data_path}')
  .writeStream
  .format('delta')
  .option("checkpointLocation",checkpoint_path)
  .trigger(availableNow=True)
  .table(table_name)  
)
stream_df.awaitTermination()

# COMMAND ----------

