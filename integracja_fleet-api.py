import json
import requests
import threading
from kafka import KafkaConsumer

KAFKA_BROKERS = ['localhost:29092', 'broker:9092'] 
FLASK_API_URL = 'http://127.0.0.1:5000'

def listen_to_alerts():
    consumer = KafkaConsumer(
        'fleet_alerts',
        bootstrap_servers=KAFKA_BROKERS,
        api_version=(0, 11, 5),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print("Odbiorca alertów uruchomiony...")
    
    for message in consumer:
        alert = message.value
        try:
            requests.post(f"{FLASK_API_URL}/alerts", json=alert)
        except requests.exceptions.ConnectionError:
            print("Błąd: API Flaska jest wyłączone!")

def listen_to_telemetry():
    consumer = KafkaConsumer(
        'vehicle_telemetry',
        bootstrap_servers=KAFKA_BROKERS,
        api_version=(0, 11, 5),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print("Odbiorca telemetrii uruchomiony...")
    
    for message in consumer:
        telemetry = message.value
        vehicle_id = telemetry.get('id_pojazdu')
        if vehicle_id:
            try:
                requests.post(f"{FLASK_API_URL}/vehicle/{vehicle_id}/update", json=telemetry)
            except requests.exceptions.ConnectionError:
                pass 

if __name__ == '__main__':
    t1 = threading.Thread(target=listen_to_alerts)
    t2 = threading.Thread(target=listen_to_telemetry)
    
    t1.start()
    t2.start()