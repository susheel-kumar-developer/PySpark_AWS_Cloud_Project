from kafka import KafkaConsumer
from kafka import KafkaProducer
import os
import json
from time import sleep
#kafka producer api .... read data from source ... to read u must write a code to read
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic = "miblr"
# data = "C:\\bigdata\\live"
data="C:\\bigdata\\live\\"
for filename in os.listdir(data):
    with open(data+str(filename), errors="ignore") as f:
      t = f.read()
      print(t)
      producer.send('miblr', t)
      sleep(5)
    #kafka store like this
    #apr4,"{results:[{...."
    #apr4,"second message"
    #apr4,"3mesage"
