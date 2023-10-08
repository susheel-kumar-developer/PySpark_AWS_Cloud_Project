from kafka import KafkaConsumer
from kafka import KafkaProducer
import os
import json
from time import sleep
#kafka producer api .... read data from source ... to read u must write a code to read
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic = "indpak"
data="C:\\bigdata\\live\\access_log_20230523-085730.log"
with open(data,mode="r", errors="ignore") as f:
    for t in f:
        print(t)
        producer.send('miblr', t)
        sleep(5)

for filename in os.listdir(data):
    with open(data+str(filename),errors="ignore") as f:
        t = f.read()
        print(t)
        producer.send()
        sleep(2)
