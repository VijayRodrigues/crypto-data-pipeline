# 🚀 Crypto Data Pipeline (Spark + Delta Lake)

---

## 📌 Overview

This project implements a **production-style incremental data pipeline** using:

- **Apache Spark (PySpark)**
- **Delta Lake**

It ingests cryptocurrency market data and processes it using a **Bronze → Silver → Gold architecture**.

---

## 🎯 Objectives

- ✅ Avoid full data reloads  
- ✅ Process only new data (incremental pipeline)  
- ✅ Ensure **idempotent writes** using Delta MERGE  
- ✅ Build **analytics-ready datasets**  

---

## 🏗️ Architecture

```
Raw Data (JSON)
↓
🟫 Bronze (raw + merge)
↓
⬜ Silver (cleaned + deduplicated)
↓
🟨 Gold (aggregated metrics)
```




---

## ⚙️ Key Features

- **Incremental Processing** using timestamp tracking  
- **Delta Lake MERGE (Upsert)** → no duplicates  
- **Partitioned Storage (date-based)**  
- **Data Validation & Cleaning**  
- **Medallion Architecture (Bronze / Silver / Gold)**  

---

## 📂 Project Structure

```
crypto-data-pipeline/
│
├── src/
│ ├── ingestion.py
│ └── process.py
│
├── metadata/
│ └── state.json
│
├── data/
│ └── processed/
│
├── requirements.txt
├── .gitignore
└── README.md
```




---

## 🥇 Bronze Layer (Raw Data)

**Purpose:** Store raw ingested data

### ✔ Features

- Stores raw JSON data  
- Uses **Delta MERGE (upsert)**  
- Prevents duplicates using `(asset_id, timestamp)`  
- Partitioned by `date`  

### 💡 Why it matters

- Acts as **source of truth**  
- Supports safe reprocessing  

---

## 🥈 Silver Layer (Cleaned Data)

**Purpose:** Improve data quality

### 🔧 Transformations

- Remove **null values**
- Filter invalid data:
  - `price_usd ≤ 0`
  - `volume_24h_usd ≤ 0`
  - `market_cap_usd ≤ 0`
- **Deduplicate records**

### 💡 Why it matters

- Ensures **reliable analytics**
- Prevents bad data from propagating

---

## 🥇 Gold Layer (Aggregated Data)

**Purpose:** Provide analytics-ready insights

### 📊 Metrics

- Average Price  
- Maximum Price  
- Minimum Price  
- Average Volume  

### 💡 Output

- One row per asset  
- Ready for dashboards  

---

## 🔄 Incremental Processing Logic

```python
timestamp > last_processed_timestamp
