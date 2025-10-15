import json
from kafka import KafkaConsumer

KAFKA_TOPIC = 'flights_raw'
KAFKA_SERVER = 'localhost:9092'

print("Inicjalizacja konsumenta...")
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest', # Kluczowy parametr: czytaj od początku
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    print(f"Nasłuchuję na temacie '{KAFKA_TOPIC}'. Czekam na wiadomości...")
    print("(Aby zatrzymać, wciśnij Ctrl+C)")

    # Ta pętla będzie działać w nieskończoność, dopóki jej nie przerwiemy
    for message in consumer:
        flight_data = message.value
        callsign = flight_data.get('callsign', 'Brak')
        country = flight_data.get('origin_country')
        print(f"Odebrano wiadomość -> Samolot: {callsign}, Kraj: {country}")

except KeyboardInterrupt:
    print("\nZatrzymywanie konsumenta.")
except Exception as e:
    print(f"Wystąpił nieoczekiwany błąd: {e}")
finally:
    # Upewniamy się, że połączenie z Kafką jest zamykane
    if 'consumer' in locals() and consumer:
        consumer.close()
        print("Konsument zamknięty.")