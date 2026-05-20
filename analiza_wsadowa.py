import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, timestamp_seconds, window, avg, desc, count

# Inicjalizacja SparkSession
spark = SparkSession.builder \
    .appName("FleetBatchAnalysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Wczytanie danych JSON

input_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dane_historyczne.json")

print(f"--- Wczytywanie danych z {input_path} ---")

try:
    df = spark.read.json(input_path)
except Exception as e:
    print(f"Błąd podczas wczytywania pliku: {e}")
    print("Upewnij się, że plik 'dane_historyczne.json' istnieje w katalogu projektu.")
    spark.stop()
    exit(1)

# Rzutowanie timestamp na typ danych Timestamp
df = df.withColumn("ts", col("timestamp").cast("timestamp"))

df.printSchema()
df.show(5)

# Identyfikacja pojazdów przekraczających prędkość (> 80 km/h)
print("\nTop 5 pojazdów najczęściej przekraczających prędkość (> 80 km/h)")
speed_violators = df.filter(col("predkosc_kmh") > 80) \
    .groupBy("id_pojazdu") \
    .agg(count("*").alias("liczba_przekroczen")) \
    .orderBy(desc("liczba_przekroczen"))

speed_violators.show(5)

# Średnie parametry dla całej floty
print("\nŚrednia temperatura silnika i prędkość na pojazd")
fleet_stats = df.groupBy("id_pojazdu") \
    .agg(
        avg("temperatura_silnika").alias("avg_temp"),
        avg("predkosc_kmh").alias("avg_speed")
    )
fleet_stats.show(10)

# Profilowanie kierowców (średnia prędkość w 5-minutowych oknach czasowych)
print("\nProfil bazowy kierowców (Średnia prędkość w 5-minutowych oknach czasowych)")
driver_profiles = df.groupBy(
    col("id_pojazdu"),
    window(col("ts"), "5 minutes").alias("okno_czasowe")
).agg(
    avg("predkosc_kmh").alias("srednia_predkosc_w_oknie")
).orderBy("id_pojazdu", "okno_czasowe")

driver_profiles.select(
    "id_pojazdu", 
    "okno_czasowe.start", 
    "okno_czasowe.end", 
    "srednia_predkosc_w_oknie"
).show(20, truncate=False)

print("\n--- Analiza zakończona sukcesem ---")
spark.stop()
