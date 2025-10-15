import time
import json
from opensky_api import OpenSkyApi
from kafka import KafkaProducer

# --- Konfiguracja ---
KAFKA_TOPIC = 'flights_raw'
KAFKA_SERVER = 'localhost:9092'

# --- Inicjalizacja ---
print("Inicjalizacja producenta Kafki...")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    # Serializuj wiadomości do formatu JSON, a następnie zakoduj do bajtów
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Inicjalizacja API OpenSky...")
api = OpenSkyApi()

print("Uruchomiono producenta. Wysyłanie danych...")
# --- Główna pętla ---
while True:
    try:
        states = api.get_states()
        if not states:
            print("Nie otrzymano danych o lotach w tym cyklu.")
            time.sleep(10)
            continue

        flight_count = 0
        for s in states.states:
            # Tworzymy prosty słownik z kluczowymi danymi o locie
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
            # Wysyłamy dane do tematu w Kafce
            producer.send(KAFKA_TOPIC, flight_data)
            flight_count += 1
        
        print(f"Wysłano dane o {flight_count} samolotach do tematu '{KAFKA_TOPIC}'.")

    except Exception as e:
        print(f"Wystąpił błąd: {e}")

    # Czekamy 15 sekund przed kolejnym zapytaniem do API
    time.sleep(15)