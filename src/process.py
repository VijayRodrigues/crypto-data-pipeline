import os
import sys
import json
import time

# =========================
# FORCE PYTHON (for pyspark stability)
# =========================
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import (
    avg, max as spark_max, min as spark_min,
    to_timestamp, col, window
)

# =========================
# PATHS
# =========================
RAW_PATH = "file:///E:/Projects/crypto-data-pipeline/data/raw_prices.json"
DELTA_PATH = "file:///E:/Projects/crypto-data-pipeline/data/processed/crypto_delta"
TIME_SERIES_PATH = "file:///E:/Projects/crypto-data-pipeline/data/processed/time_series_delta"
STATE_PATH = r"E:\Projects\crypto-data-pipeline\metadata\state.json"

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("crypto-delta-incremental") \
    .master("local[*]") \
    .config("spark.local.dir", "E:/Projects/crypto-data-pipeline/spark-temp") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# SCHEMA
# =========================
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("asset_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("market_cap_usd", DoubleType(), True),
    StructField("market_cap_rank", IntegerType(), True),
    StructField("volume_24h_usd", DoubleType(), True),
    StructField("circulating_supply", DoubleType(), True),
    StructField("total_supply", DoubleType(), True),
    StructField("max_supply", DoubleType(), True),
    StructField("ath_usd", DoubleType(), True),
    StructField("ath_change_pct", DoubleType(), True),
    StructField("high_24h_usd", DoubleType(), True),
    StructField("low_24h_usd", DoubleType(), True)
])

# =========================
# LOAD STATE
# =========================
if os.path.exists(STATE_PATH):
    with open(STATE_PATH, "r") as f:
        state = json.load(f)
        last_processed_timestamp = state["last_processed_timestamp"]
else:
    last_processed_timestamp = "1970-01-01T00:00:00"

print("Last processed timestamp:", last_processed_timestamp)

# =========================
# READ DATA
# =========================
df = spark.read.schema(schema).json(RAW_PATH)

# =========================
# FIX TIMESTAMP
# =========================
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# =========================
# FILTER NEW DATA (INCREMENTAL)
# =========================
df_new = df.filter(col("timestamp") > last_processed_timestamp)

count_new = df_new.count()
print("New records count:", count_new)

if count_new == 0:
    print("No new data. Exiting.")
    spark.stop()
    exit()

# =========================
# CLEAN DATA
# =========================
df_clean = df_new.dropna(subset=["price_usd", "asset_id"])

# =========================
# AGGREGATION
# =========================
df_agg = df_clean.groupBy("asset_id").agg(
    avg("price_usd").alias("avg_price"),
    spark_max("price_usd").alias("max_price"),
    spark_min("price_usd").alias("min_price"),
    avg("volume_24h_usd").alias("avg_volume")
)

# =========================
# TIME-BASED AGGREGATION
# =========================
df_time = df_clean.groupBy(
    window("timestamp", "1 minute"),
    "asset_id"
).agg(
    avg("price_usd").alias("avg_price")
)

# =========================
# WRITE DELTA (APPEND MODE NOW)
# =========================
df_agg.write \
    .format("delta") \
    .mode("append") \
    .save(DELTA_PATH)

df_time.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("asset_id") \
    .save(TIME_SERIES_PATH)

print("Data written successfully")

# =========================
# UPDATE STATE
# =========================
max_ts = df_new.select(spark_max("timestamp")).collect()[0][0]

if max_ts:
    new_ts = max_ts.isoformat()
    with open(STATE_PATH, "w") as f:
        json.dump({"last_processed_timestamp": new_ts}, f)

    print("Updated last_processed_timestamp:", new_ts)

# =========================
# STOP
# =========================
spark.stop()
time.sleep(2)