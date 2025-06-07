# Databricks notebook source
import requests
import json

# COMMAND ----------

DATABRICKS_INSTANCE = "https://e2-demo-field-eng.cloud.databricks.com"
token = dbutils.secrets.get( "dais-2025-json-parsing-demo", "token")

# COMMAND ----------

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

payload = {
    "name": "from_json_no_schema",
    "catalog": "murali_talluri",
    "schema": "priming_schema",
    "libraries": [
        {
            "notebook": {
                "path": "/Users/murali.talluri@databricks.com/DAIS-demo-JSON-schema-handling/from_json-DLT/from_json-no-schema"
            }
        }
    ],
    "clusters": [
        {
            "label": "default",
            "node_type_id": "i3.2xlarge",
            "num_workers": 3
        }
    ],
    "development": True,
    "continuous": False,
    "edition": "ADVANCED",
    "channel": "CURRENT"
}

response = requests.post(
    f"{DATABRICKS_INSTANCE}/api/2.0/pipelines",
    headers=headers,
    data=json.dumps(payload)
)

print(response.status_code)
print(response.json())


# COMMAND ----------

payload2 = {
    "name": "from_json_prime_schema",
    "catalog": "murali_talluri",
    "schema": "priming_schema",
    "libraries": [
        {
            "notebook": {
                "path": "/Users/murali.talluri@databricks.com/DAIS-demo-JSON-schema-handling/from_json-DLT/from_json-prime-schema"
            }
        }
    ],
    "clusters": [
        {
            "label": "default",
            "node_type_id": "i3.2xlarge",
            "num_workers": 3
        }
    ],
    "development": True,
    "continuous": False,
    "edition": "ADVANCED",
    "channel": "CURRENT"
}

response2 = requests.post(
    f"{DATABRICKS_INSTANCE}/api/2.0/pipelines",
    headers=headers,
    data=json.dumps(payload2)
)

print(response2.status_code)
print(response2.json())


# COMMAND ----------

