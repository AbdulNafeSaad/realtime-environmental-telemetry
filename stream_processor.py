import os
import glob
import urllib.request
import jdk

# --- 1. Automated Windows Environment Configuration (JDK 11 & Hadoop Winutils) ---
java_exe = next(
    (p for pat in [r"C:\Program Files\Eclipse Adoptium\**\java.exe", os.path.expanduser(r"~\.jdk\**\java.exe")]
     for p in glob.glob(pat, recursive=True)),
    None
)
if not java_exe:
    print("Downloading OpenJDK 11...")
    jdk.install("11")
    java_exe = glob.glob(os.path.expanduser(r"~\.jdk\**\java.exe"), recursive=True)[0]

java_home = os.path.dirname(os.path.dirname(java_exe))
os.environ["JAVA_HOME"] = java_home

hadoop_home = os.path.expanduser(r"~\.hadoop")
hadoop_bin = os.path.join(hadoop_home, "bin")
os.makedirs(hadoop_bin, exist_ok=True)

base_url = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
for filename in ["winutils.exe", "hadoop.dll"]:
    target_path = os.path.join(hadoop_bin, filename)
    if not os.path.exists(target_path):
        urllib.request.urlretrieve(f"{base_url}/{filename}", target_path)

os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = (
    os.path.join(java_home, "bin") + os.pathsep +
    hadoop_bin + os.pathsep +
    os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, stddev, max as spark_max,
    count, round as spark_round, when, expr, coalesce, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# --- 2. Initialize Spark Session ---
spark = SparkSession.builder \
    .appName("EnvironmentalTelemetryAnomalyEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 3. Define Telemetry Schema & Stream Source ---
USE_KAFKA = False  # Set to True when local Kafka broker is running on localhost:9092
OUTPUT_CSV = "anomalies_detected.csv"

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("pm25_reading", DoubleType()),
    StructField("humidity", DoubleType())
])

if USE_KAFKA:
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor_telemetry") \
        .option("startingOffsets", "latest") \
        .load()

    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
else:
    # Built-in simulator: generates baseline telemetry with periodic PM2.5 spikes
    parsed_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 4) \
        .load() \
        .select(
            expr("CASE WHEN value % 3 = 0 THEN 'NODE_NORTH_01' "
                 "WHEN value % 3 = 1 THEN 'NODE_SOUTH_02' "
                 "ELSE 'NODE_CENTRAL_03' END").alias("sensor_id"),
            col("timestamp"),
            expr("115.0 + 18.0 * sin(value) + "
                 "CASE WHEN value % 11 = 0 THEN 95.0 ELSE 0.0 END").cast("double").alias("pm25_reading"),
            expr("55.0 + 10.0 * cos(value)").cast("double").alias("humidity")
        )

# --- 4. Sliding Window Aggregation & Z-Score Anomaly Detection ---
windowed_stats = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("sensor_id")
    ) \
    .agg(
        count("pm25_reading").alias("sample_count"),
        spark_round(avg("pm25_reading"), 2).alias("mean_pm25"),
        spark_round(coalesce(stddev("pm25_reading"), lit(0.0)), 2).alias("std_pm25"),
        spark_round(spark_max("pm25_reading"), 2).alias("peak_pm25"),
        spark_round(avg("humidity"), 1).alias("mean_humidity")
    )

anomalies = windowed_stats.withColumn(
    "peak_z_score",
    when(col("std_pm25") > 0, spark_round((col("peak_pm25") - col("mean_pm25")) / col("std_pm25"), 2))
    .otherwise(lit(0.0))
).filter(
    (col("mean_pm25") > 150.0) | (col("peak_z_score") >= 2.0)
).withColumn(
    "severity",
    when((col("peak_pm25") >= 200.0) & (col("peak_z_score") >= 2.5), lit("CRITICAL_SPIKE"))
    .when(col("mean_pm25") > 150.0, lit("SUSTAINED_HAZARD"))
    .otherwise(lit("STATISTICAL_OUTLIER"))
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "sensor_id",
    "sample_count",
    "mean_pm25",
    "peak_pm25",
    "std_pm25",
    "peak_z_score",
    "mean_humidity",
    "severity"
)

# --- 5. Dual Sink: Real-Time Console Display + Persistent CSV Logging ---
def process_anomaly_batch(batch_df, batch_id):
    # Convert Spark TimestampType columns to strings to avoid Pandas 2.x datetime64 casting errors
    formatted_batch_df = batch_df \
        .withColumn("window_start", col("window_start").cast("string")) \
        .withColumn("window_end", col("window_end").cast("string"))
    
    pdf = formatted_batch_df.toPandas()
    if not pdf.empty:
        print(f"\n--- Anomaly Micro-Batch {batch_id} ---")
        batch_df.show(truncate=False)
        write_header = not os.path.exists(OUTPUT_CSV)
        pdf.to_csv(OUTPUT_CSV, mode="a", header=write_header, index=False)

query = anomalies.writeStream \
    .outputMode("update") \
    .foreachBatch(process_anomaly_batch) \
    .start()

# Runs for 60 seconds during local testing; remove the number to run indefinitely
query.awaitTermination(60)
query.stop()
spark.stop()
print(f"Stream finished. Anomalies saved to {OUTPUT_CSV}.")