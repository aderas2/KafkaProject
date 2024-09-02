import json
import logging
from kafka import KafkaConsumer
import streamlit as st

st.title('Visualizing stream')
display = st.empty()
displayText = ''
consumer = KafkaConsumer('S1_TRANSFORMED',value_deserializer=lambda m: json.loads(m.decode('ascii')), auto_offset_reset='earliest')
for msg in consumer:
    logging.info (msg.value)
    data = msg.value
    #concatenate with current displayText and pass to st's display created above
    displayText+="{}\n".format(data)
    display.text(displayText)