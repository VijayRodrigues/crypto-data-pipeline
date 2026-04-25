import requests
import time
import json
import os
import random
from datetime import datetime

# =========================
# CONFIG
# =========================
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
COINS = "bitcoin,ethereum,binancecoin,solana,ripple"
VS_CURRENCY = "usd"

INTERVAL = 15          # final decision
RATE_LIMIT_SLEEP = 60  # backoff for 429

OUTPUT_PATH = os.path.join("data", "raw_prices.json")


# =========================
# FETCH DATA
# =========================
def fetch_data():
    params = {
        "vs_currency": VS_CURRENCY,
        "ids": COINS
    }

    try:
        response = requests.get(API_URL, params=params, timeout=10)

        if response.status_code == 429:
            print(f"[{datetime.utcnow()}] Rate limit hit. Sleeping {RATE_LIMIT_SLEEP}s...")
            time.sleep(RATE_LIMIT_SLEEP)
            return None

        if response.status_code != 200:
            raise Exception(f"API Error: {response.status_code} - {response.text}")

        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"[{datetime.utcnow()}] Request error: {e}")
        time.sleep(5)
        return None


# =========================
# TRANSFORM DATA
# =========================
def transform_data(data):
    records = []
    timestamp = datetime.utcnow().isoformat()

    for coin in data:
        record = {
            "timestamp": timestamp,

            "asset_id": coin.get("id"),
            "symbol": coin.get("symbol"),
            "name": coin.get("name"),

            "price_usd": coin.get("current_price"),
            "market_cap_usd": coin.get("market_cap"),
            "market_cap_rank": coin.get("market_cap_rank"),

            "volume_24h_usd": coin.get("total_volume"),

            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply"),
            "max_supply": coin.get("max_supply"),

            "ath_usd": coin.get("ath"),
            "ath_change_pct": coin.get("ath_change_percentage"),

            "high_24h_usd": coin.get("high_24h"),
            "low_24h_usd": coin.get("low_24h")
        }

        records.append(record)

    return records


# =========================
# WRITE DATA
# =========================
def write_data(records):
    os.makedirs("data", exist_ok=True)

    with open(OUTPUT_PATH, "a") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


# =========================
# MAIN LOOP
# =========================
def run():
    print("Starting ingestion...")

    while True:
        data = fetch_data()

        if data is None:
            continue

        try:
            records = transform_data(data)
            write_data(records)

            print(f"[{datetime.utcnow()}] Ingested {len(records)} records")

            # jitter to avoid exact pattern
            time.sleep(INTERVAL + random.uniform(0, 2))

        except Exception as e:
            print(f"[{datetime.utcnow()}] Processing error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    run()