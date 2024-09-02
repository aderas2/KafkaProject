import json
from kafka import KafkaConsumer
consumer = KafkaConsumer('json-events',value_deserializer=lambda m: json.loads(m.decode('ascii')), auto_offset_reset='earliest')
for msg in consumer:
    print (msg.value)
