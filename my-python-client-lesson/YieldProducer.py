from confluent_kafka import Producer
import numpy as np
import time
import json
import pandas as pd
import psycopg2

# Load the dataset
file_path = r'C:\Users\SST-LAB\Desktop\Bilau-DAT608\DAT608Project\yielddata.csv'
data = pd.read_csv(file_path)

# Configure the Kafka producer (if still needed for other tasks)
producer_conf = {
    'bootstrap.servers': 'localhost:49092'  # Ensure this is the correct address for your Kafka broker
}
producer = Producer(producer_conf)

# Database configuration
DB_CONFIG = {
    'dbname': 'yield_data_db',
    'user': 'bilau',
    'password': 'cockroach',
    'host': 'localhost',
    'port': '8880'
}

# Function to generate synthetic data
def generate_synthetic_data():
    # Generate synthetic temperature and pesticides
    temperature = np.random.normal(data['Temperature (Celsius)'].mean(), data['Temperature (Celsius)'].std())
    pesticides = np.random.normal(data['Pesticides (tonnes)'].mean(), data['Pesticides (tonnes)'].std())
    return {
        'Temperature (Celsius)': temperature,
        'Pesticides (tonnes)': pesticides
    }

# Function to handle delivery reports (for Kafka)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Function to insert data into CockroachDB
def insert_data_to_db(data):
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO yield_data_db.yield_data (temperature, pesticides, yield, data_source)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (data['Temperature (Celsius)'], data['Pesticides (tonnes)'], None, 'synthetic')
                )
                conn.commit()
                print("Data inserted into database.")
    except Exception as e:
        print(f"An error occurred while inserting data into database: {e}")

# Produce data to Kafka (if still needed)
while True:
    try:
        synthetic_data = generate_synthetic_data()
        # Produce message to Kafka (if still needed)
        producer.produce('yield_data_topic', key="synthetic", value=json.dumps(synthetic_data), callback=delivery_report)
        producer.flush()  # Ensure all messages are delivered
        
        # Insert data directly into CockroachDB
        insert_data_to_db(synthetic_data)
        
        time.sleep(5)  # Wait before generating the next message
    except KeyboardInterrupt:
        print("Producer interrupted by user.")
        break
    except Exception as e:
        print(f"An error occurred: {e}")
