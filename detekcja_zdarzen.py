from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, max as spark_max, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("FleetEventDetection") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id_pojazdu", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("predkosc_kmh", IntegerType(), True),
        StructField("temperatura_silnika", IntegerType(), True),
        StructField("poziom_paliwa_procent", IntegerType(), True),
        StructField("GPS_lat", DoubleType(), True),
        StructField("GPS_lon", DoubleType(), True)
    ])

    KAFKA_BOOTSTRAP_SERVERS = "broker:9092"

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "vehicle_telemetry") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    watermarked_stream = parsed_stream.withWatermark("timestamp", "2 minutes")

    aggregated_stream = watermarked_stream.groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("id_pojazdu")
    ).agg(
        avg("predkosc_kmh").alias("srednia_predkosc"),
        spark_max("temperatura_silnika").alias("max_temperatura")
    )

    alerts_stream = aggregated_stream.filter(
        (col("max_temperatura") > 110) | (col("srednia_predkosc") > 130)
    )

    output_stream = alerts_stream.select(
        col("id_pojazdu").alias("key"), # Klucz wiadomości na Kafce
        to_json(struct(
            col("id_pojazdu"),
            col("window.end").alias("czas_zdarzenia"),
            col("srednia_predkosc"),
            col("max_temperatura"),
            lit("CRITICAL_ALERT").alias("typ_alarmu")
        )).alias("value")
    )

    query = output_stream.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", "fleet_alerts") \
        .option("checkpointLocation", "/tmp/spark_checkpoints_fleet_alerts") \
        .outputMode("update") \
        .start()

    print("Rozpoczęto nasłuchiwanie strumienia i detekcję zdarzeń...")
    query.awaitTermination()

if __name__ == "__main__":
    main()