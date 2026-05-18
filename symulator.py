import time
import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    api_version=(0, 11, 5), 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TELEMETRY_TOPIC = 'vehicle_telemetry'
ALERTS_TOPIC = 'fleet_alerts'

lista_pojazdow = [
    "VOLVO_001", "SCANIA_002", "MAN_003", "DAF_004",
    "MERCEDES_005", "IVECO_006", "RENAULT_007", "VOLVO_008",
    "SCANIA_009", "MAN_010", "DAF_011", "MERCEDES_012",
    "IVECO_013", "RENAULT_014", "FORD_015", "SCANIA_016",
    "VOLVO_017", "MAN_018", "DAF_019", "IVECO_020"
]

try:
    while True:
        pojazd = random.choice(lista_pojazdow)
        
        obecny_czas = datetime.now()
        temperatura = random.randint(70, 95) 
        event_type = "normal"
        opis_alertu = None
        
        # --- ALERTY ---
        los = random.randint(1, 100)
        
        if los <= 5: 
            # 5% szans: pojazd wjechał do tunelu i wysłał dane z opóźnieniem (np. sprzed 2 minut)
            obecny_czas = obecny_czas - timedelta(minutes=2)
            event_type = "late_signal"
            opis_alertu = "Spozniony sygnal - prawdopodobna utrata zasiegu"
            print("📡 TUNEL: Utrata zasięgu! Wysyłam spóźniony sygnał...")
            
        elif los >= 95:
            # 5% szans: awaria silnika, drastyczny skok temperatury
            temperatura = random.randint(115, 140)
            event_type = "engine_overheat"
            opis_alertu = "Awaria - nagly skok temperatury silnika"
            print("⚠️ AWARIA: Wykryto przegrzanie silnika!")
        # -----------------------------------------------
        
        dane_z_czujnikow = {
            "id_pojazdu": pojazd,
            "timestamp": obecny_czas.isoformat(),
            "event_type": event_type,
            "predkosc_kmh": random.randint(0, 100),
            "temperatura_silnika": temperatura,
            "poziom_paliwa_procent": random.randint(5, 100),
            "GPS_lat": round(random.uniform(50.0, 54.0), 4),
            "GPS_lon": round(random.uniform(14.0, 24.0), 4)
        }
        
        producer.send(TELEMETRY_TOPIC, value=dane_z_czujnikow)
        print(f"Wysłano: {dane_z_czujnikow}")

        if event_type != "normal":
            alert = {
                "id_pojazdu": pojazd,
                "timestamp": datetime.now().isoformat(),
                "event_type": event_type,
                "alert_level": "HIGH" if event_type == "engine_overheat" else "MEDIUM",
                "message": opis_alertu,
                "source_topic": TELEMETRY_TOPIC,
                "temperatura_silnika": temperatura
            }
            producer.send(ALERTS_TOPIC, value=alert)
            print(f"Alert wyslany do {ALERTS_TOPIC}: {alert}")
        
        time.sleep(1)

except KeyboardInterrupt:
    print("\nZatrzymano symulator.")
finally:
    producer.close()