import streamlit as st
import folium
from streamlit_folium import folium_static
from kafka import KafkaConsumer
import json
from threading import Thread
import time

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'ISS_LOCATION_PROCESSED',
    bootstrap_servers=['broker:29092'],  # Replace with your Kafka broker address
    value_deserializer=lambda m: json.loads(m.decode('ascii')),
    auto_offset_reset='latest',
    group_id=None
)

# Initialize global variables
latest_lat = 0.0
latest_lon = 0.0

def kafka_consumer():
    global latest_lat, latest_lon
    for message in consumer:
        try:
            # Parse the message value
            last_message = message.value
            # Update global variables with the latest location
            latest_lat = float(last_message['LATITUDE'])
            latest_lon = float(last_message['LONGITUDE'])
            print(f"Updated location: LAT={latest_lat}, LON={latest_lon}")  # Debugging output
        except Exception as e:
            print(f"Error processing message: {e}")

# Start Kafka consumer in a separate thread
consumer_thread = Thread(target=kafka_consumer, daemon=True)
consumer_thread.start()

st.title('Real-Time ISS Location Tracker')

# Create a placeholder for the map
map_placeholder = st.empty()

while True:
    # Ensure the global variables are not zero (indicating data hasn't been updated)
    if latest_lat != 0.0 and latest_lon != 0.0:
        # Initialize the map centered at the latest location
        m = folium.Map(location=[latest_lat, latest_lon], zoom_start=3)
        folium.Marker([latest_lat, latest_lon], popup="ISS Current Location").add_to(m)
    
        # Display the map in the Streamlit app
        with map_placeholder.container():
            folium_static(m)
    
    else:
        # Display a message if no location data is available
        st.write("Waiting for ISS location data...")

    # Short delay before the next update
    time.sleep(5)  # Adjust the interval as needed
