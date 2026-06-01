from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'fleet_alerts', 
    bootstrap_servers=['broker:9092']
)

print("Nasłuchuję tematu 'fleet_alerts'... (Czekam na wiadomości od Sparka)")

for msg in consumer:
    print(msg.value.decode('utf-8'))