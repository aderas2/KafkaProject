import streamlit as st
from confluent_kafka import Consumer, KafkaError
import json
import pandas as pd
import psycopg2
import joblib
import time

# Kafka Consumer Configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:49092',
    'group.id': 'streamlit-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['yield_data_topic'])

# Database configuration
conn = psycopg2.connect(
    dbname='yield_data_db',
    user='bilau',
    password='cockroach',
    host='localhost',
    port="8880"
)
cursor = conn.cursor()

# Load your trained model (update the path as needed)
model = joblib.load(r'C:\Users\SST-LAB\Desktop\Bilau-DAT608\DAT608Project\random_forest_model.pkl')

# Function to consume data from Kafka topic
def consume_kafka_data(consumer):
    msg = consumer.poll(timeout=0.0)  # Timeout of 1 second
    if msg is None:
        return None
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition, no more messages
            return None
        else:
            st.error(f"Kafka error: {msg.error()}")
            return None
    else:
        data_point = json.loads(msg.value().decode('utf-8'))
        return data_point

# Streamlit interface for real-time predictions
st.title("Real-Time Yield Prediction")

if st.button("Start Consuming Data"):
    placeholder = st.empty()
    try:
        while True:
            new_data = consume_kafka_data(consumer)
            if new_data:
                placeholder.write(f"Consumed Data: {new_data}")
                
                # Insert consumed data into CockroachDB
                try:
                    cursor.execute(
                        """
                        INSERT INTO yield_data_db.yield_data (temperature, pesticides, yield, data_source)
                        VALUES (%s, %s, %s, %s);
                        """,
                        (new_data['Temperature (Celsius)'], new_data['Pesticides (tonnes)'], None, 'generated')
                    )
                    conn.commit()
                except psycopg2.Error as db_error:
                    st.error(f"Database error: {db_error}")
                
                # Predict yield
                try:
                    new_data_df = pd.DataFrame([new_data])
                    prediction = model.predict(new_data_df)
                    placeholder.write(f"Predicted Yield: {prediction[0]}")
                except Exception as pred_error:
                    st.error(f"Prediction error: {pred_error}")
            
            # Optional: Add a sleep interval to control polling frequency
            time.sleep(5)
    except KeyboardInterrupt:
        st.info("Stopped consuming data.")
    except Exception as e:
        st.error(f"An error occurred: {e}")
    finally:
        # Close the consumer and database connection
        consumer.close()
        cursor.close()
        conn.close()
