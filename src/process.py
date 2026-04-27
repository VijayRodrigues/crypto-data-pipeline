import os
import sys
import json
import time

# =========================
# FORCE PYTHON (Spark)
# =========================
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# =========================
# BASE PATH (CRITICAL FIX)
# =========================
BASE_DIR = "/app"

RAW_PATH = f"{BASE_DIR}/data/raw_prices.json"

BRONZE_PATH = f"{BASE_DIR}/data/processed/crypto_bronze"
SILVER_PATH = f"{BASE_DIR}/data/processed/crypto_silver"
GOLD_PATH   = f"{BASE_DIR}/data/processed/crypto_gold"

STATE_PATH = f"{BASE_DIR}/metadata/state.json"

# Ensure directories exist
os.makedirs(f"{BASE_DIR}/metadata", exist_ok=True)
os.makedirs(f"{BASE_DIR}/data/processed", exist_ok=True)

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("crypto-medallion-pipeline") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.local.dir", "/tmp/spark-temp") \
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
        last_processed_timestamp = state.get("last_processed_timestamp", "1970-01-01T00:00:00")
else:
    last_processed_timestamp = "1970-01-01T00:00:00"

print("Last processed timestamp:", last_processed_timestamp)

# =========================
# READ DATA
# =========================
if not os.path.exists(RAW_PATH):
    print("Raw file not found, skipping processing.")
    spark.stop()
    sys.exit(0)

df = spark.read.schema(schema).json(RAW_PATH)

df = df.withColumn("timestamp", to_timestamp("timestamp"))
df = df.withColumn("date", to_date("timestamp"))

# =========================
# INCREMENTAL FILTER
# =========================
df_new = df.filter(col("timestamp") > last_processed_timestamp)
new_count = df_new.count()

print("New records:", new_count)

# ==========================================================
# 🥇 BRONZE
# ==========================================================
if new_count > 0:
    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        print("Creating Bronze...")
        df_new.write.format("delta").mode("overwrite").partitionBy("date").save(BRONZE_PATH)
    else:
        print("Merging into Bronze...")
        bronze = DeltaTable.forPath(spark, BRONZE_PATH)

        bronze.alias("t").merge(
            df_new.alias("s"),
            "t.asset_id = s.asset_id AND t.timestamp = s.timestamp"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

# ==========================================================
# 🥈 SILVER
# ==========================================================
if new_count > 0:
    df_silver = df_new \
        .dropna(subset=["price_usd", "asset_id"]) \
        .filter(col("price_usd") > 0) \
        .filter(col("volume_24h_usd") > 0) \
        .filter(col("market_cap_usd") > 0) \
        .dropDuplicates(["asset_id", "timestamp"])

    df_silver.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("date") \
        .save(SILVER_PATH)

    print("Silver updated")

# ==========================================================
# 🥇 GOLD
# ==========================================================
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    df_full = spark.read.format("delta").load(SILVER_PATH)

    df_gold = df_full.groupBy("asset_id").agg(
        avg("price_usd").alias("avg_price"),
        max("price_usd").alias("max_price"),
        min("price_usd").alias("min_price"),
        avg("volume_24h_usd").alias("avg_volume")
    )

    df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .save(GOLD_PATH)

    print("Gold updated")

# ==========================================================
# UPDATE STATE
# ==========================================================
if new_count > 0:
    max_ts = df_new.select(max("timestamp")).collect()[0][0]
    if max_ts:
        with open(STATE_PATH, "w") as f:
            json.dump({"last_processed_timestamp": max_ts.isoformat()}, f)

# ==========================================================
# VALIDATION
# ==========================================================
print("\n=== DATA VALIDATION ===")

if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    df_b = spark.read.format("delta").load(BRONZE_PATH)
    print("Bronze count:", df_b.count())

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    print("Silver count:", spark.read.format("delta").load(SILVER_PATH).count())

if DeltaTable.isDeltaTable(spark, GOLD_PATH):
    print("Gold count:", spark.read.format("delta").load(GOLD_PATH).count())

# =========================
# STOP
# =========================
spark.stop()
time.sleep(2)