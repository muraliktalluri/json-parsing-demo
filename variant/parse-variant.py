# Databricks notebook source
from pyspark.sql.functions import schema_of_variant_agg, col, replace, lit, expr
from pyspark.sql.types import StructType

# COMMAND ----------

table_name = 'murali_talluri.priming_schema.bronze'
output_schema = 'murali_talluri.priming_schema'

# COMMAND ----------

schema_df = (
  spark.table(table_name)
  .withColumn('schema_type', expr('data:type::int'))
  .groupBy('schema_type')
  .agg(
    schema_of_variant_agg(col('data')).alias('schema')
  )
  .withColumn(
    'schema',replace(col('schema'), lit('OBJECT'), lit('STRUCT'))
  )
)
for id, schema in schema_df.collect():
  parsed_df = (spark.table(table_name)
    .filter(expr('data:type::int')==id)
    .selectExpr(f'data::{schema}')
    .select('data.*')
  )
  (parsed_df.write.format('delta')
   .saveAsTable(f'{output_schema}.silver_{id}')
  )

# COMMAND ----------

