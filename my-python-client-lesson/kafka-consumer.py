from kafka import KafkaConsumer
import json
import pandas as pd
data_list = []
message = 0

consumer = KafkaConsumer('yield_data_topic',
						 bootstrap_servers=['broker:39092'], 
						 auto_offset_reset='earliest')

for msg in consumer:
	message += 1
	data = json.loads(msg.value)
	data_list.append(data)
	print(message)

	if(message > 20):
		break

print("Gotten Data")
df = pd.DataFrame(data_list)
print(df)