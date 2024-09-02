from kafka import KafkaConsumer
from cockroach_connect import getConnection
import logging
from datetime import datetime
import re
import json

# Setup logging
logger = logging.getLogger('cockroachdb_kafka')
logger.setLevel(logging.INFO)  # Set to INFO level

# Get mandatory connection
conn = getConnection(True)

def sanitize_text(text):
    if text is None:
        return None
    
    return re.sub(r'[^\x20-\x7E]', '', text)

def setup_database():
    """Create the table if it does not exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hardware_events (
                        id SERIAL PRIMARY KEY,
                        timestamp TIMESTAMP,
                        temperature FLOAT,
                        humidity FLOAT,
                        pressure FLOAT
                )
            """)
            conn.commit()
            logger.info("Table created or already exists.")
    except Exception as e:
        logger.error(f"Problem setting up the database: {e}")

def cockroachWrite(event):
    """Write Kafka event to the database."""
    try:
        # Read and convert data from event
        data = json.loads(event.value)
        
        # Extract values
        timestamp = datetime.strptime(data['timestamp'], "%Y-%m-%d %H:%M:%S")
        temperature = data['temperature']
        humidity = data['humidity']
        pressure = data['pressure']

        # Insert into the database
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO hardware_events (timestamp, temperature, humidity, pressure) 
                    VALUES (%s, %s, %s, %s)
                """, (timestamp, temperature, humidity, pressure))
            conn.commit()
            logger.info("Data inserted successfully.")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
    except KeyError as e:
        logger.error(f"Missing key in JSON data: {e}")
    except Exception as e:
        logger.error(f"Problem writing to database: {e}")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    "hardware_data",
    bootstrap_servers=['broker:39092'], 
    auto_offset_reset='earliest' # Replace with your Kafka server details
)

# Setup the database once
setup_database()

# Process messages
for msg in consumer:
    cockroachWrite(msg)
    logger.info("Msg seen.") 
