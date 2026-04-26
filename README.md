# Crypto Data Pipeline (Spark + Delta Lake)

<h2>📌 Overview</h2>

<p>
This project implements an <b>incremental data pipeline</b> using <b>Apache Spark (PySpark)</b> and <b>Delta Lake</b>.
</p>

<p>
It ingests cryptocurrency market data and processes it using a <b>Bronze–Silver–Gold architecture</b>.
</p>

<ul>
  <li>Incremental processing</li>
  <li>Delta Lake MERGE (upsert)</li>
  <li>Data cleaning + validation</li>
  <li>Aggregated analytics</li>
</ul>

---

## 🏗️ Architecture

```
Raw Data (JSON)
↓
Bronze (raw + merge)
↓
Silver (cleaned + deduped)
↓
Gold (aggregated metrics)
```



---

## ⚙️ Layers

### 🥇 Bronze
<ul>
  <li>Raw data storage</li>
  <li>MERGE (upsert)</li>
  <li>No duplicates</li>
</ul>

### 🥈 Silver
<ul>
  <li>Removes nulls</li>
  <li>Filters invalid values</li>
  <li>Deduplicates data</li>
</ul>

### 🥇 Gold
<ul>
  <li>Aggregated metrics per asset</li>
  <li>avg_price, max_price, min_price, avg_volume</li>
</ul>

---

## 🚀 Features

- Incremental processing using timestamp
- Delta Lake MERGE
- Partitioned storage
- Data validation checks
- Medallion architecture

---

## ▶️ How to Run

```bash
pip install -r requirements.txt
python src/ingestion.py
python src/process.py
