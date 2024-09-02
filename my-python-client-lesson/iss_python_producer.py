import requests
import json
from kafka import KafkaProducer
import time

def get_iss_location():
    response = requests.get("http://api.open-notify.org/iss-now.json")
    return response.json()

producer = KafkaProducer(bootstrap_servers=['broker:29092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    iss_data = get_iss_location()
    producer.send('iss_location', iss_data)
    time.sleep(8)