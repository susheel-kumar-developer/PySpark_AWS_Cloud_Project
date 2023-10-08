from time import sleep
from json import dumps
from kafka import KafkaProducer
import os
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
# Define the location of the directory

#producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
# logs file should be there
path="C:\\\logs\\"
file_list = os.listdir(path)
for a_file in file_list:
    with open(path+str(a_file),errors="ignore") as f:
        line = f.read()
        print(line)
        producer.send("may25",line)
        sleep(4)