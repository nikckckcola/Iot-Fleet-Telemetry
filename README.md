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
## 5. Moduł: Ciągłe Przetwarzanie Sygnału

Moduł odpowiada za odbiór, czyszczenie i agregację surowego strumienia danych telemetrycznych w czasie rzeczywistym przy użyciu PySpark Structured Streaming.

**Plik realizujący ten etap:**

'streaming_app.py' – główna aplikacja strumieniowa.

**Główne mechanizmy wdrożone w module:**

1. **Odczyt strumienia z Kafki:** Aplikacja podłącza się do tematu vehicle_telemetry i odbiera dane w trybie ciągłym. Surowe bajty są dekodowane i parsowane do struktury DataFrame zgodnie z predefiniowanym schematem, identycznym z formatem wiadomości generowanych przez symulator.
2. **Czyszczenie danych w locie:** Przed agregacją każdy rekord jest walidowany. Odrzucane są rekordy z ujemną prędkością (błąd sensora), współrzędnymi GPS poza dopuszczalnym zakresem (symulator generuje wartość 999.0 jako symulację awarii GPS), brakującym identyfikatorem pojazdu lub znacznikiem czasu, oraz temperaturą silnika poza fizycznym zakresem 0–200°C.
3. **Sliding Windows (Okna Przesuwne):** Oczyszczone dane są grupowane per pojazd w wędrujących oknach czasowych 30-sekundowych przesuwanych co 10 sekund. W każdym oknie obliczana jest średnia prędkość, maksymalna temperatura silnika oraz średni poziom paliwa. Zastosowanie max zamiast avg dla temperatury pozwala wykryć nawet chwilowe skoki wartości w obrębie okna.
4. **Watermarking (Obsługa Opóźnień):** Moduł uwzględnia scenariusz utraty zasięgu — zezwala na 2-minutowe opóźnienie w napływie pakietów (zdarzenie late_signal generowane przez symulator przy wjeździe do tunelu). Spóźnione dane są poprawnie przypisywane do odpowiednich historycznych okien czasowych na podstawie event time, a nie czasu przetwarzania.

---
### 6. Moduł 4: Detekcja Zdarzeń w Czasie Rzeczywistym (Stream Processing)

Moduł ten odpowiada za ciągłe analizowanie strumienia danych telemetrycznych na żywo przy użyciu **PySpark Structured Streaming**. Jego głównym celem jest wykrywanie niebezpiecznych wzorców w zachowaniu floty (np. przegrzewanie silnika) i wysyłanie alertów do systemu.

**Pliki realizujące ten etap:**
* `detekcja_zdarzen.py` – główny silnik analityczny Spark.
* `sprawdz_alerty.py` – skrypt konsumenta pełniący rolę podglądu dla dyspozytora.

**Główne mechanizmy wdrożone w module:**
1. **Sliding Windows (Okna Przesuwne):** Dane są grupowane w oknach 30-sekundowych, które przesuwają się co 10 sekund. W każdym oknie obliczana jest m.in. średnia prędkość oraz maksymalna temperatura silnika dla danego pojazdu.
2. **Watermarking (Obsługa Opóźnień):** Skrypt uwzględnia "problem tunelu" – zezwala na 2-minutowe opóźnienie w napływie pakietów (np. gdy pojazd odzyska zasięg i wyśle zaległe logi). Spóźnione dane są poprawnie dopisywane do odpowiednich historycznych okien czasowych.
3. **Detekcja Anomalii:** Jeśli w danym oknie czasowym średnia prędkość przekroczy 130 km/h LUB maksymalna temperatura przekroczy 110 stopni, generowany jest `CRITICAL_ALERT`.

---

### Moduł 5: Serwer Flask + API

Ostatni z modułów odpowiada za stworzenie serwera opartego o technologię Flask, oraz interfejsu API pozwalającego na:
1) Za pomocą metod typu POST - uploadowanie symulowanych danych i alertów na serwer
2) GET - uzyskanie dostępu do interesujących nas danych

Plik integracja_fleet-api.py automatyzuje pobieranie pojawiających się na kafkowych wątkach danych, oraz postowanie ich na naszym serwerze, właśnie za pomocą metod API.

---

## 5. Instrukcja obsługi systemu

### Krok 1: Pozyskiwanie danych (Przygotowanie wsadu)
1. Uruchomienie symulatora w terminalu: `python symulator.py`
2. Uruchomienie zbieracza danych w nowym terminalu: `python zbieracz_danych.py`
   *Zatrzymanie zbieracza (`Ctrl+C`) po zebraniu odpowiedniej ilości danych (plik `dane_historyczne.json`).*

### Krok 2: Uruchomienie analiz analitycznych
Przetwarzanie danych można uruchomić na dwa sposoby:

**Sposób A (Przez JupyterLab - zalecany):**
1. Otwórz `http://localhost:8999` (hasło: `root`).
2. Przejdź do folderu ze skryptami.
3. Otwórz Terminal (**File > New > Terminal**) i wpisz:
   ```bash
   spark-submit analiza_wsadowa.py
   ```

**Sposób B (Przez terminal systemowy):**
```bash
docker exec -it iot-fleet-telemetry-spark-1 spark-submit analiza_wsadowa.py
```
*Skrypt automatycznie wykryje plik `dane_historyczne.json` w tym samym folderze dzięki dynamicznemu ustalaniu ścieżek.*

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
