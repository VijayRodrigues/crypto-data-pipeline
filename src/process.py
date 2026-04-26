import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg, max as spark_max, min as spark_min, to_timestamp, window

# =========================
# SPARK SESSION (DELTA ENABLED)
# =========================
spark = SparkSession.builder \
    .appName("crypto-delta-pipeline") \
    .master("local[*]") \
    .config("spark.local.dir", "E:/Projects/crypto-data-pipeline/spark-temp") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

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
# READ DATA
# =========================
file_path = "file:///E:/Projects/crypto-data-pipeline/data/raw_prices.json"

df = spark.read \
    .schema(schema) \
    .json(file_path)

# =========================
# CLEAN + FIX TIMESTAMP
# =========================
df_clean = df.dropna(subset=["price_usd", "asset_id"])

df_clean = df_clean.withColumn(
    "timestamp",
    to_timestamp("timestamp")
)

# =========================
# AGGREGATION (OVERALL)
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
# WRITE DELTA (GOLD TABLE)
# =========================
delta_path = "file:///E:/Projects/crypto-data-pipeline/data/processed/crypto_delta"

df_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_path)

# =========================
# WRITE DELTA (TIME SERIES)
# =========================
df_time.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("asset_id") \
    .save("file:///E:/Projects/crypto-data-pipeline/data/processed/time_series_delta")

# =========================
# VERIFY DELTA (VERSIONING)
# =========================
print("\nDelta History:")
spark.sql(f"DESCRIBE HISTORY delta.`{delta_path}`").show(truncate=False)

# =========================
# READ BACK DELTA
# =========================
df_check = spark.read.format("delta").load(delta_path)

print("\nRead Back Data:")
df_check.show(5, truncate=False)

# =========================
# STOP
# =========================
spark.stop()