from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

producer.send('quickstart-events', b'message-from-python')

for n in range(3):
    message = 'Even more additional message right in class {} from python'.format(n)
    producer.send('quickstart-events', bytearray(message, 'utf-8'))

producer.flush() # producer.send() is asynchronous. Flush all messages to topic.

import msgpack
import json
# encode objects via msgpack. See https://msgpack.org/index.html
producer2 = KafkaProducer(value_serializer=msgpack.dumps)
producer2.send('msgpack-events', {'msgpack_key': 'msgpack_value'})
producer2.flush() # block until all async messages are sent
# produce json messages
producer3 = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
producer3.send('json-events', {'json_key': 'json_value'})
producer3.flush() # block until all async messages are sent
