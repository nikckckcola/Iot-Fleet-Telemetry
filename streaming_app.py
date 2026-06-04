from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, avg,
    max as spark_max,
    window, to_json, struct, lit
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# ── Konfiguracja ──────────────────────────────────────────────
KAFKA_BROKER  = "broker:9092"
TEMAT_WEJSCIE = "vehicle_telemetry"
TEMAT_ALARMY  = "fleet_alerts"

OKNO_DLUGOSC  = "30 seconds"
OKNO_KROK     = "10 seconds"
WATERMARK     = "2 minutes"

PROG_PREDKOSCI   = 130  # km/h
PROG_TEMPERATURY = 110  # °C

# ── Schemat — identyczny z symulator.py ──────────────────────
SCHEMAT_TELEMETRII = StructType([
    StructField("id_pojazdu",            StringType(),    True),
    StructField("timestamp",             TimestampType(), True),
    StructField("event_type",            StringType(),    True),
    StructField("predkosc_kmh",          IntegerType(),   True),
    StructField("temperatura_silnika",   IntegerType(),   True),
    StructField("poziom_paliwa_procent", IntegerType(),   True),
    StructField("GPS_lat",               DoubleType(),    True),
    StructField("GPS_lon",               DoubleType(),    True),
])


def stworz_sesje():
    return (
        SparkSession.builder
        .appName("FlotaStreaming_Lab3")
        .getOrCreate()
    )


def czytaj_ze_strumienia(spark):
    surowy = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TEMAT_WEJSCIE)
        .option("startingOffsets", "latest")
        .load()
    )
    return (
        surowy
        .select(from_json(col("value").cast("string"), SCHEMAT_TELEMETRII).alias("d"))
        .select("d.*")
    )


def czysc_dane(df):
    return df.filter(
        col("timestamp").isNotNull() &
        col("id_pojazdu").isNotNull() &
        (col("id_pojazdu") != "") &
        col("predkosc_kmh").isNotNull() &
        (col("predkosc_kmh") >= 0) &
        col("GPS_lat").isNotNull() &
        col("GPS_lon").isNotNull() &
        col("GPS_lat").between(-90, 90) &
        col("GPS_lon").between(-180, 180) &
        col("temperatura_silnika").isNotNull() &
        col("temperatura_silnika").between(0, 200)
    )


def agreguj_w_oknie(df):
    return (
        df
        .withWatermark("timestamp", WATERMARK)
        .groupBy(
            window(col("timestamp"), OKNO_DLUGOSC, OKNO_KROK),
            col("id_pojazdu")
        )
        .agg(
            avg("predkosc_kmh").alias("srednia_predkosc"),
            spark_max("temperatura_silnika").alias("max_temperatura"),
            avg("poziom_paliwa_procent").alias("sredni_poziom_paliwa")
        )
    )


def wykryj_alarmy(df_okno):
    alarmy = df_okno.filter(
        (col("max_temperatura") > PROG_TEMPERATURY) |
        (col("srednia_predkosc") > PROG_PREDKOSCI)
    )
    return alarmy.select(
        col("id_pojazdu").alias("key"),
        to_json(struct(
            col("id_pojazdu"),
            col("window.end").alias("czas_zdarzenia"),
            col("srednia_predkosc"),
            col("max_temperatura"),
            col("sredni_poziom_paliwa"),
            lit("CRITICAL_ALERT").alias("typ_alarmu"),
            lit("lab3_streaming").alias("zrodlo")
        )).alias("value")
    )


def main():
    spark = stworz_sesje()
    spark.sparkContext.setLogLevel("WARN")

    # Pipeline
    surowe  = czytaj_ze_strumienia(spark)
    czyste  = czysc_dane(surowe)
    z_oknem = agreguj_w_oknie(czyste)
    alarmy  = wykryj_alarmy(z_oknem)

    # Wyjście A: wyniki na konsolę
    zapytanie_konsola = (
        z_oknem.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Wyjście B: alarmy → Kafka fleet_alerts
    zapytanie_alarmy = (
        alarmy.writeStream
        .outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", TEMAT_ALARMY)
        .option("checkpointLocation", "/tmp/checkpoint_lab3_alarmy")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("=" * 55)
    print("Lab 3 — Spark Structured Streaming URUCHOMIONY")
    print(f"  Wejście  : {TEMAT_WEJSCIE} @ {KAFKA_BROKER}")
    print(f"  Alarmy   : {TEMAT_ALARMY}")
    print(f"  Okna     : {OKNO_DLUGOSC} / co {OKNO_KROK}")
    print(f"  Watermark: {WATERMARK}")
    print(f"  Progi    : temp > {PROG_TEMPERATURY}C | predkosc > {PROG_PREDKOSCI} km/h")
    print("=" * 55)
    print("Czekam na dane... (Ctrl+C zeby zatrzymac)")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
