# Inteligentny System Telemetrii i Monitorowania Floty Pojazdów (IoT)

Kompleksowy system monitorowania floty w czasie rzeczywistym oraz analizy danych IoT. Projekt ma strukturę modułową, pozwalającą na niezależny rozwój poszczególnych komponentów systemu (symulacja, zbieranie danych, analiza batchowa, analiza strumieniowa).

---

## 1. Cel projektu

Celem projektu jest zasymulowanie i analiza strumienia danych, który mógłby pochodzić z czujników zamontowanych w pojazdach ciężarowych. System pozwala na monitorowanie parametrów technicznych w czasie rzeczywistym, wykrywanie awarii oraz tworzenie historycznych profili zachowań kierowców.

### Symulowane dane obejmują:
- Identyfikator pojazdu i aktualny znacznik czasu.
- Prędkość pojazdu (km/h) i poziom paliwa (%).
- Temperaturę silnika oraz lokalizację GPS na terenie Polski.

### Zdarzenia specjalne:
- **`late_signal`** - symulacja utraty zasięgu (np. w tunelu) – dane przesyłane z opóźnieniem.
- **`engine_overheat`** - nagły skok temperatury silnika oznaczający usterkę techniczną.

---

## 2. Architektura i wspólna infrastruktura

### Wykorzystane technologie:
- **Broker wiadomości**: Apache Kafka & ZooKeeper
- **Silnik analityczny**: Apache Spark (PySpark)
- **Konteneryzacja**: Docker & Docker Compose
- **Język programowania**: Python 3.x

### Uruchomienie środowiska:
Wszystkie moduły korzystają ze wspólnej infrastruktury zdefiniowanej w `docker-compose.yml`.
```bash
docker compose up -d
```
*Dostęp do środowiska JupyterLab: `http://localhost:8999` (hasło: `root`).*

---

## 3. Moduł: Symulacja sensorów IoT

Odpowiada za generowanie realistycznego strumienia danych z pojazdów i wysyłanie ich do systemu.

- **Skrypt**: `symulator.py`
- **Główne zadania**: 
    - Generowanie losowych odczytów telemetrycznych w formacie JSON.
    - Wysyłanie danych do tematów `vehicle_telemetry` (dane bieżące) oraz `fleet_alerts` (powiadomienia o usterkach).
- **Format wiadomości**:
```json
{
  "id_pojazdu": "VOLVO_001",
  "timestamp": "2026-05-18T21:39:00",
  "event_type": "normal",
  "predkosc_kmh": 82,
  "temperatura_silnika": 94,
  "poziom_paliwa_procent": 67,
  "GPS_lat": 52.1234,
  "GPS_lon": 19.5678
}
```

---

## 4. Moduł: Raportowanie i profilowanie floty

Odpowiada za analizę danych historycznych (wsadową) w celu wyznaczenia wzorców zachowań i raportowania anomalii.

- **Skrypty**: `zbieracz_danych.py` (pozyskiwanie danych), `analiza_wsadowa.py` (analiza Spark).
- **Zrealizowane raporty**:
    - **Data Lake**: Składowanie danych strumieniowych w formacie JSON Lines na potrzeby audytu.
    - **Analiza prędkości**: Identyfikacja pojazdów najczęściej przekraczających dozwolone normy.
    - **Profilowanie bazowe**: Wykorzystanie funkcji okien czasowych (Window Functions) do wyliczenia średniej prędkości w trasach (np. w oknach 15-minutowych), co pozwala określić "normę" dla danego kierowcy.

---

## 5. Instrukcja obsługi systemu

### Krok 1: Pozyskiwanie danych (Przygotowanie wsadu)
1. Uruchomienie symulatora w terminalu: `python symulator.py`
2. Uruchomienie zbieracza danych w nowym terminalu: `python zbieracz_danych.py`
   *Zatrzymanie zbieracza (`Ctrl+C`) po zebraniu odpowiedniej ilości danych (plik `dane_historyczne.json`).*

### Krok 2: Uruchomienie analiz analitycznych
Przetwarzanie danych zgromadzonych w pliku wykonuje się poleceniem:
```bash
docker exec -it iot-fleet-telemetry-spark-1 spark-submit /home/jovyan/work/analiza_wsadowa.py
```

---

## Struktura plików projektu

```text
.
├── docker-compose.yml     # Wspólna konfiguracja kontenerów
├── symulator.py           # [Moduł 1] Generator danych
├── zbieracz_danych.py     # [Moduł 2] Pozyskiwanie danych do JSON
├── analiza_wsadowa.py     # [Moduł 2] Analiza Spark Batch
├── dane_historyczne.json  # [Dane] Plik wynikowy pozyskiwania danych
└── README.md              # Główna dokumentacja
```

---

## Uwagi do dalszego tworzenia projektu
System jest przygotowany na dodanie kolejnych modułów (np. część 3 - analiza strumieniowa, część 4: wizualizacja danych). Każdy nowy moduł powinien korzystać z istniejącego brokera wiadomości i być udokumentowany w analogiczny sposób.
