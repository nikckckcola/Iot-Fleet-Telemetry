import json
from kafka import KafkaConsumer

# Konfiguracja konsumenta
TELEMETRY_TOPIC = 'vehicle_telemetry'
BOOTSTRAP_SERVERS = ['localhost:29092', 'broker:9092'] # Obsługuje uruchamianie na hoście i w Dockerze

consumer = KafkaConsumer(
    TELEMETRY_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest', # od początku dostępnych danych
    enable_auto_commit=True,
    group_id='history_collector_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

FILENAME = 'dane_historyczne.json'

print(f"Rozpoczynam zbieranie danych z tematu '{TELEMETRY_TOPIC}'...")
print(f"Dane zostaną zapisane w pliku: {FILENAME}")
print("Naciśnij Ctrl+C, aby przerwać i zapisać dane.")

count = 0
try:
    with open(FILENAME, 'a', encoding='utf-8') as f:
        for message in consumer:
            data = message.value
            f.write(json.dumps(data) + '\n')
            count += 1
            if count % 10 == 0:
                print(f"Odebrano {count} wiadomości...")
except KeyboardInterrupt:
    print(f"\nZatrzymano zbieranie. Łącznie zebrano {count} wiadomości.")
finally:
    consumer.close()
