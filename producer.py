import time
import json
from opensky_api import OpenSkyApi
from kafka import KafkaProducer

# configuration
KAFKA_TOPIC = 'flights_raw'
KAFKA_SERVER = 'localhost:9092'

# --- initialization ---
print("Inicjalizacja producenta Kafki...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    # Serialize messages to JSON and then encode them to bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Inicjalizacja API OpenSky...")
api = OpenSkyApi()

print("Uruchomiono producenta. Wysyłanie danych...")
# main loop
while True:
    try:
        states = api.get_states()
        if not states:
            print("Nie otrzymano danych o lotach w tym cyklu.")
            time.sleep(10)
            continue

        flight_count = 0
        for s in states.states:
            # dict
            flight_data = {
                'icao24': s.icao24,
                'callsign': s.callsign.strip() if s.callsign else None,
                'origin_country': s.origin_country,
                'longitude': s.longitude,
                'latitude': s.latitude,
                'baro_altitude': s.baro_altitude,
                'on_ground': s.on_ground,
                'velocity': s.velocity,
                'true_track': s.true_track,
                'vertical_rate': s.vertical_rate
            }
            # sending data to kafka topic
            producer.send(KAFKA_TOPIC, flight_data)
            flight_count += 1
        
        print(f"Wysłano dane o {flight_count} samolotach do tematu '{KAFKA_TOPIC}'.")

    except Exception as e:
        print(f"Wystąpił błąd: {e}")

    
    time.sleep(15)