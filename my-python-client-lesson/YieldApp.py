import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import joblib
import json
from db_writer import cockroachWriteFrontEnd
from kafka import KafkaConsumer
from confluent_kafka import Consumer, KafkaError

consumer = KafkaConsumer(
    'yield_data_topic',  # Ensure this matches the topic used in your producer
    bootstrap_servers=['localhost:49092'],  # Update with your Kafka server address
    auto_offset_reset='earliest', 
    enable_auto_commit=True  # Automatically commit offsets
)

def fetch_data():
    for msg in consumer:
        eventValue = json.loads(msg.value)

        temperature = eventValue['temperature']
        pesticides = eventValue['pesticides']


# Load the trained model
try:
    model = joblib.load('../random_forest_model.pkl')
except Exception as e:
    st.error(f"Error loading model: {e}")
    st.stop()

# User input
user_temperature = st.number_input("Temperature (Celsius)", value=25.0)
user_pesticides = st.number_input("Pesticides (tonnes)", value=2.0)

if st.button('Predict'):
    # Make a prediction based on user input
    user_data = pd.DataFrame({
        'Temperature (Celsius)': [user_temperature],
        'Pesticides (tonnes)': [user_pesticides]
    })
    try:
        user_prediction = model.predict(user_data)
        st.write(f"Predicted Yield (hg/ha): {user_prediction[0]:.2f}")
    except Exception as e:
        st.error(f"Error making prediction: {e}")

    cockroachWriteFrontEnd(user_temperature, user_pesticides, user_prediction[0])



# Plot the results
st.subheader("Model Performance")


