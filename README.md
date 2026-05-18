# Symulator danych telemetrycznych pojazdow

Projekt przedstawia prosty symulator danych w czasie rzeczywistym. Skrypt w Pythonie generuje losowe odczyty telemetryczne dla floty pojazdow ciezarowych i wysyla je do Apache Kafka w formacie JSON.

## Cel projektu

Celem projektu jest zasymulowanie strumienia danych, ktory moglby pochodzic z czujnikow zamontowanych w pojazdach. Takie dane moga byc dalej przetwarzane, analizowane lub wizualizowane w systemach do analizy danych w czasie rzeczywistym.

Przykladowe dane obejmuja:

- identyfikator pojazdu,
- aktualny znacznik czasu,
- typ zdarzenia,
- predkosc pojazdu,
- temperature silnika,
- poziom paliwa,
- przykladowa lokalizacje GPS na terenie Polski.

Symulator generuje rowniez zdarzenia specjalne:

- `late_signal` - spozniony sygnal, np. po utracie zasiegu w tunelu,
- `engine_overheat` - nagly skok temperatury silnika oznaczajacy usterke.

## Technologie

- Python
- Apache Kafka
- ZooKeeper
- Docker Compose
- biblioteka `kafka-python`

## Struktura projektu

```text
.
├── docker-compose.yml   # konfiguracja Kafki i ZooKeepera
├── symulator.py         # generator danych telemetrycznych
└── README.md            # opis projektu
```

## Wymagania

Przed uruchomieniem projektu nalezy miec zainstalowane:

- Docker,
- Docker Compose,
- Python 3,
- biblioteke `kafka-python`.

Biblioteke Pythona mozna zainstalowac poleceniem:

```bash
pip install kafka-python
```

## Uruchomienie

1. Uruchom Kafke i ZooKeepera:

```bash
docker compose up -d
```

Podczas startu tworzona jest rowniez para topicow Kafka:

```text
vehicle_telemetry
fleet_alerts
```

2. Uruchom symulator:

```bash
python symulator.py
```

Po uruchomieniu skrypt bedzie co sekunde wysylal nowa wiadomosc do topicu Kafka:

```text
vehicle_telemetry
```

Jesli zostanie wykryty spozniony sygnal lub awaria, dodatkowa wiadomosc zostanie wyslana do topicu:

```text
fleet_alerts
```

W terminalu powinny pojawiac sie komunikaty podobne do:

```text
Wyslano: {'id_pojazdu': 'VOLVO_001', 'timestamp': '2026-05-18T21:39:00', 'predkosc_kmh': 82, 'temperatura_silnika': 94, 'poziom_paliwa_procent': 67, 'GPS_lat': 52.1234, 'GPS_lon': 19.5678}
```

3. Aby zatrzymac symulator, wcisnij:

```text
CTRL+C
```

4. Aby zatrzymac kontenery Dockera:

```bash
docker compose down
```

## Format wysylanej wiadomosci

Kazda wiadomosc telemetryczna wysylana do `vehicle_telemetry` ma format JSON:

```json
{
  "id_pojazdu": "VOLVO_001",
  "timestamp": "2026-05-18T21:39:00.000000",
  "event_type": "normal",
  "predkosc_kmh": 82,
  "temperatura_silnika": 94,
  "poziom_paliwa_procent": 67,
  "GPS_lat": 52.1234,
  "GPS_lon": 19.5678
}
```

Przykladowa wiadomosc alertowa wysylana do `fleet_alerts`:

```json
{
  "id_pojazdu": "VOLVO_001",
  "timestamp": "2026-05-18T21:39:00.000000",
  "event_type": "engine_overheat",
  "alert_level": "HIGH",
  "message": "Awaria - nagly skok temperatury silnika",
  "source_topic": "vehicle_telemetry",
  "temperatura_silnika": 128
}
```

## Opis dzialania

Skrypt `symulator.py` tworzy producenta Kafka, ktory laczy sie z brokerem pod adresem `localhost:29092`. Nastepnie w petli nieskonczonej losuje pojazd z listy, generuje przykladowe wartosci czujnikow i wysyla je do topicu `vehicle_telemetry`.

W kazdej iteracji istnieje niewielka szansa na wygenerowanie zdarzenia specjalnego. Dla spoznionego sygnalu czas zdarzenia jest cofany o 2 minuty, co pozwala testowac przetwarzanie opoznionych danych. Dla awarii silnika temperatura jest podnoszona do zakresu `115-140`, co symuluje usterke. Takie zdarzenia trafiaja takze do topicu `fleet_alerts`.

Program dziala do momentu recznego zatrzymania przez uzytkownika.

## Uwaga

Jesli symulator nie moze polaczyc sie z Kafka, sprawdz konfiguracje portow w `docker-compose.yml` oraz adres `bootstrap_servers` w pliku `symulator.py`. Obecnie skrypt probuje laczyc sie z Kafka przez `localhost:29092`.
