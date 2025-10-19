# producer.py (Simulator Mode)

import time
import json
from kafka import KafkaProducer

# --- Configuration ---
KAFKA_TOPIC = 'flights_raw'
KAFKA_SERVER = 'localhost:9092'
DATA_SOURCE_FILE = 'sample_flights.json'  # Make sure this matches your filename

# --- Initialization ---
print("Initializing Kafka Producer...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    # Serialize messages to JSON format, then encode to bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Producer started in SIMULATOR mode. Reading from '{DATA_SOURCE_FILE}'...")

# --- Main Loop ---
while True:
    try:
        with open(DATA_SOURCE_FILE, 'r') as f:
            flight_count = 0
            for line in f:
                try:
                    # Read one flight data from the file
                    flight_data = json.loads(line)
                    
                    # Send it to Kafka
                    producer.send(KAFKA_TOPIC, flight_data)
                    flight_count += 1
                    
                    # Wait a little to simulate a real-time stream
                    time.sleep(0.05)
                except json.JSONDecodeError:
                    # Ignore empty or malformed lines in the file
                    continue
            
            print(f"Sent data for {flight_count} aircraft. Restarting from beginning of file...")
    
    except FileNotFoundError:
        print(f"❌ ERROR: Data source file not found: '{DATA_SOURCE_FILE}'")
        print("Please make sure the file exists and has the correct name.")
        break
    except Exception as e:
        print(f"An error occurred: {e}")
        time.sleep(5)