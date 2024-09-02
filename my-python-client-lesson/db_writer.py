from kafka import KafkaConsumer
from cockroach_connect import getConnection
import logging
from datetime import datetime
import re
import json

# Setup logging
logging.basicConfig(level=logging.INFO)

# Get mandatory connection
conn = getConnection(True)

def sanitize_text(text):
    if text is None:
        return None
    # Remove non-printable characters except for standard whitespace
    return re.sub(r'[^\x20-\x7E]', '', text)

def setup_database():
    """Create the table if it does not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS yield_table (
                    id SERIAL PRIMARY KEY,
                    temperature FLOAT,
                    pesticides FLOAT,
                    yield FLOAT
                )
            """)
            conn.commit()
            logging.debug("Table created or already exists.")
    except Exception as e:
        logging.error(f"Problem setting up the database: {e}")

def cockroachWrite(event):
    """Write Kafka event to the database."""
    try:
        eventValue = json.loads(event.value)

        temperature = eventValue['temperature']
        pesticides = eventValue['pesticides']

        # Sanitize eventValue to remove NUL bytes
        eventValue = sanitize_text(eventValue)

        # Insert into the database
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO yield_table (temperature, pesticides) 
                VALUES (%s, %s)
            """, (temperature, pesticides))
            conn.commit()
            logging.debug("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Problem writing to database: {e}")

def cockroachWriteFrontEnd(temperature, pesticides, f_yield):
    """Write Kafka event to the database."""
    try:
        # Setup the database once
        setup_database()
        # Insert into the database
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO yield_table (temperature, pesticides, yield) 
                VALUES (%s, %s, %s)
            """, (temperature, pesticides, f_yield))
            conn.commit()
            logging.info("Data inserted successfully.")
    except Exception as e:
        logging.error(f"Problem writing to database: {e}")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'yield_data_topic',  # Ensure this matches the topic used in your producer
    bootstrap_servers=['localhost:49092'],  # Update with your Kafka server address
    auto_offset_reset='earliest', 
    group_id='consumer-group',  # Add group ID to manage offsets
    enable_auto_commit=True  # Automatically commit offsets
)


