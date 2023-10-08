from kafka import KafkaConsumer

consumer = KafkaConsumer('indpak', group_id='my_favorite_group')
for msg in consumer:
    print (msg.value)
