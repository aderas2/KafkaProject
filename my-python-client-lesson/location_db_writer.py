from kafka import KafkaConsumer
from cockroach_connect import getConnection
import logging
from datetime import datetime
import json 

# Setup logging
logging.basicConfig(level=logging.DEBUG)

# Get mandatory connection
conn = getConnection(True)


def setup_database():
    """Create the table if it does not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS iss_locations (
                    timestamp TIMESTAMPTZ,
                    latitude FLOAT,
                    longitude FLOAT
                )
            """)
            conn.commit()
            logging.debug("Table created or already exists.")
    except Exception as e:
        logging.error(f"Problem setting up the database: {e}")

def cockroachWrite(event):
    """Write Kafka event to the database."""
    try:
        # Decode and parse JSON event
        event_data = json.loads(event.value.decode('utf-8', errors='replace'))
        
        # Extract fields from JSON event
        eventTimestamp = event_data.get('timestamp')
        latitude = float(event_data.get('latitude'))
        longitude = float(event_data.get('longitude'))

        # Insert into the database
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO iss_locations (timestamp, latitude, longitude) 
                VALUES (%s, %s, %s)
            """, (eventTimestamp, latitude, longitude))
            conn.commit()
            logging.debug("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Problem writing to database: {e}")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'iss_location_processed',
    bootstrap_servers=['broker:29092']
)

# Setup the database once
setup_database()

# Process messages
for msg in consumer:
    cockroachWrite(msg)
    logging.debug("Msg seen.")
