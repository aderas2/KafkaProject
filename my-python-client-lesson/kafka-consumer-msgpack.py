import msgpack
from kafka import KafkaConsumer
consumer = KafkaConsumer('msgpack-events', value_deserializer=msgpack.unpackb, auto_offset_reset='earliest')
for msg in consumer:
	print (msg.value)
