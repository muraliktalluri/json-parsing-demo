# Databricks notebook source
# MAGIC %pip install kafka-python

# COMMAND ----------

from kafka import KafkaProducer
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "dais-2025-json-parsing-demo", "prime-schema-from-json")

# Kafka topics
topic_name = 'from_json_test_topic_schema3'

# COMMAND ----------

# Function to generate fake data
def generate_fake_data():
    return {
        "string_field": random.choice(['example_'+str(i) for i in range(10000)]),  # String field
        "int_field": random.randint(100, 500),  # Integer field
        "int_field2": random.choice(['mismatch_'+str(i) for i in range(10000)]),  # Integer field2
        "double_field": random.uniform(1.5, 9.5),  # Double (floating-point) field        
        "array_field": [random.randint(1, 100) for _ in range(5)],
        "dict_field": {
            "field1": random.randint(1, 100),
            "field2": random.uniform(2.5, 4.5),
            "field3": random.choice(['value_'+str(i) for i in range(1000)]),
            "field4": [random.randint(101, 500) for _ in range(5)]
        },
        "nested_dict_field": {
            "field1": random.choice(['nested_'+str(i) for i in range(1000)]),
            "field2": {
                "nested_field1": random.randint(101, 200),
                "nested_field2": random.uniform(4.5, 6.5) 
            }
        },
        "nested_array_field" : [
            {
                "nested_field1": random.choice(['nested_'+str(i) for i in range(1,100)]),  # String field
                "nested_field2": random.randint(201, 300),  # Integer field
                "nested_field3": random.uniform(6.5, 8.5),  # Double (floating-point) field        
                "nested_field4": [random.randint(201, 300) for _ in range(5)],
            },
            {
                "nested_field1": random.choice(['nested_'+str(i) for i in range(100,200)]),  # String field
                "nested_field2": random.randint(301, 400),  # Integer field
                "nested_field3": random.uniform(8.5, 10.5),  # Double (floating-point) field        
                "nested_field4": [random.randint(301, 400) for _ in range(5)],
            }         
        ]
    }

# Serialize JSON data
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers_plaintext.split(','),  # Update Kafka broker IP and port
    value_serializer=json_serializer
)

def send_data():
    try:
        while True:
            data = generate_fake_data()
            # print(f"Sending data to {topic_name}: {data}")
            producer.send(topic_name, data)
            # time.sleep(1)  # Send data every second
    except KeyboardInterrupt:
        print(f"Stopped producing data to {topic_name}.")

# Creating threads for each topic
thread1 = threading.Thread(target=send_data)
thread2 = threading.Thread(target=send_data)
thread3 = threading.Thread(target=send_data)
thread4 = threading.Thread(target=send_data)
thread5 = threading.Thread(target=send_data)
thread6 = threading.Thread(target=send_data)
thread7 = threading.Thread(target=send_data)
thread8 = threading.Thread(target=send_data)
thread9 = threading.Thread(target=send_data)
thread10 = threading.Thread(target=send_data)

# Starting threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
thread7.start()
thread8.start()
thread9.start()
thread10.start()

# Waiting for threads to complete (optional, remove if script should run indefinitely)
thread1.join()
thread2.join()
thread3.join()
thread4.join()
thread5.join()
thread6.join()
thread7.join()
thread8.join()
thread9.join()
thread10.join()

# COMMAND ----------

