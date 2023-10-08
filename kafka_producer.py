
from kafka import KafkaConsumer
from kafka import KafkaProducer
import os
import json
from time import sleep
#kafka producer api .... read data from source ... to read u must write a code to read
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))

data="C:\\bigdata\\live\\access_log_20230524-075006.log"
with open(data,mode="r", errors="ignore") as f:
    for t in f:
        print(t)
        producer.send('indpak', t)
        sleep(5)

