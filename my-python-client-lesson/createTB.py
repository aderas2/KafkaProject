import psycopg2
from psycopg2 import sql

# Database configuration
DB_CONFIG = {
    'dbname': 'postgres',  # Default database to connect for creation
    'user': 'bilau',
    'password': 'cockroach',
    'host': 'localhost',
    'port': '8880'
}

# Create database and table
def create_database_and_table():
    try:
        # Connect to the default database
        with psycopg2.connect(**DB_CONFIG) as conn:
            conn.autocommit = True  # Enable autocommit to create database
            with conn.cursor() as cur:
                # Create database if it does not exist
                cur.execute("SELECT 1 FROM pg_database WHERE datname = 'yield_data_db'")
                if not cur.fetchone():
                    cur.execute("CREATE DATABASE yield_data_db")
                    print("Database 'yield_data_db' created.")
                else:
                    print("Database 'yield_data_db' already exists.")
                
                # Close the connection to connect to the new database
                conn.close()

        # Connect to the new database to create the table
        DB_CONFIG['dbname'] = 'yield_data_db'
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Create table if it does not exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS yield_data (
                        id SERIAL PRIMARY KEY,
                        temperature FLOAT NOT NULL,
                        pesticides FLOAT NOT NULL,
                        yield FLOAT,
                        data_source TEXT NOT NULL
                    )
                """)
                conn.commit()
                print("Table 'yield_data' created or already exists.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Run the function
create_database_and_table()
