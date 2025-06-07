# Advanced JSON schema handling

## Description
This demo explains: 
- How to from_json is typically used for parsing JSON data. 
- How Variant simplifies JSON ingestion and processing.
- How from_json in DLT is used for automatic schema inference and evolution of JSON data. 
- How to prime your schema and pass it to from_json as schema hint for better schema inference and avoiding multiple restarts. 

## Folder structure
### Gen and Send Json Data to Kafka
Generates random JSON data and write to a Kafka topic.

### from_json_non-DLT
Shows the 3 approaches of how from_json(without DLT) is used for processing JSON data.

### variant
- **create-variant-json-data**: Create JSON data with 100 different schemas.
- **variant-ingest**: Ingest JSON data(bronze) as Variant using Autoloader. 
- **parse-variant**: Demux the variant data using schema_of_variant_agg.

### from_json-DLT
- **create-DLT-pipelines**: Creates DLT pipelines
- **from_json-no-schema**: Code for the DLT pipeline that doesn't use use schema hint.
- **prime-schema-of-json-agg**: Priming your schema using schema_of_json_agg.
- **from_json-prime-schema**: Code for the DLT pipeline that uses schema hint, the result of schema_of_json_agg.