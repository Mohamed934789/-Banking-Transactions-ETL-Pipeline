# 🏦 Banking Transactions ETL Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4-orange?style=for-the-badge&logo=apachespark)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.4-017CEE?style=for-the-badge&logo=apacheairflow)
![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-3.2.1-yellow?style=for-the-badge&logo=apachehadoop)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=for-the-badge&logo=snowflake)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker)

**A fully automated, production-grade ETL pipeline for banking transaction analytics**

*Real-time simulation → HDFS → PySpark → Snowflake → Ready for BI dashboards*

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Star Schema Design](#-star-schema-design)
- [Pipeline Steps](#-pipeline-steps)
- [Getting Started](#-getting-started)
- [Running the Pipeline](#-running-the-pipeline)
- [Airflow DAG](#-airflow-dag)
- [Services & Ports](#-services--ports)
- [Dataset](#-dataset)
- [Screenshots](#-screenshots)

---

## 🎯 Overview

This project implements a **full ETL (Extract, Transform, Load) pipeline** for banking transaction data using modern data engineering tools.

### What it does:
1. **Simulates** real-time banking transactions from a Kaggle dataset
2. **Extracts** data from a landing zone into **HDFS** (Hadoop Distributed File System)
3. **Transforms** raw data using **PySpark** — cleaning, deduplication, and building a **Star Schema**
4. **Loads** the structured data into **Snowflake** Cloud Data Warehouse
5. **Orchestrates** the entire pipeline with **Apache Airflow** running on a daily schedule

### Business Value:
> The goal is to give the **Data Analyst team** a clean, optimized data model they can query efficiently — transforming messy raw transactions into a structured Star Schema ready for reporting and business intelligence.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Environment                        │
│                                                                  │
│  📁 Kaggle CSV                                                   │
│       │                                                          │
│       ▼  simulate_transactions.py (batches every 60s)           │
│  📂 Landing Zone (/data/landing/)                               │
│       │                                                          │
│       ▼  [Airflow Task 1] extract.py → WebHDFS API             │
│  🗄️  HDFS Raw  (hdfs://namenode:9000/banking/raw/date=...)     │
│       │                                                          │
│       ▼  [Airflow Task 2] spark/transform.py (PySpark)         │
│  🗄️  HDFS Staged (/banking/staged/) — Parquet / Star Schema   │
│       │                                                          │
│       ▼  [Airflow Task 3] spark/load.py → Snowflake connector  │
│  ❄️  Snowflake  (BANKING_DWH.STAR_SCHEMA)                      │
│                                                                  │
│  ⏰  Apache Airflow — Orchestrates everything (daily @ 00:00)  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Version | Role |
|------|---------|------|
| **Apache Hadoop (HDFS)** | 3.2.1 | Distributed file system for raw data storage |
| **Apache YARN** | 3.2.1 | Cluster resource manager |
| **Apache Spark (PySpark)** | 3.4 | Distributed data processing & transformation |
| **Apache Airflow** | 2.10.4 | Pipeline orchestration & scheduling |
| **Snowflake** | Cloud | Data Warehouse — final destination |
| **PostgreSQL** | 13 | Airflow metadata database |
| **Docker Compose** | — | Container orchestration |
| **Python** | 3.12 | Scripting language |
| **Pandas** | ≥2.2.0 | Data manipulation |

---

## 📁 Project Structure

```
banking_etl_project/
│
├── 📄 docker-compose.yml           # All services definition
├── 📄 .env                         # Snowflake credentials (not committed)
├── 📄 .gitignore
│
├── 📂 docker/
│   └── 📄 Dockerfile.airflow       # Custom Airflow image with Java + PySpark
│
├── 📂 data/
│   ├── 📂 raw/
│   │   └── bank_transactions.csv   # Source dataset from Kaggle
│   ├── 📂 landing/                 # Simulation output (batches)
│   └── 📂 processed/               # After successful extract
│
├── 📂 scripts/
│   ├── 📄 simulate_transactions.py # Splits CSV into batches (real-time sim)
│   └── 📄 extract.py               # Uploads batches to HDFS via WebHDFS
│
├── 📂 spark/
│   ├── 📄 transform.py             # PySpark: clean data + build Star Schema
│   └── 📄 load.py                  # Load Parquet from HDFS → Snowflake
│
├── 📂 dags/
│   └── 📄 banking_etl_dag.py       # Airflow DAG definition
│
└── 📂 notebooks/
    └── 📄 transform_exploration.ipynb  # Jupyter development notebook
```

---

## ⭐ Star Schema Design

The transformation layer builds a **Star Schema** optimized for analytical queries:

```
                    ┌─────────────────────┐
                    │      DIM_DATE        │
                    │─────────────────────│
                    │ date_id (PK)         │
                    │ date                 │
                    │ year / month / day   │
                    │ day_name             │
                    │ month_name           │
                    │ quarter              │
                    └──────────┬──────────┘
                               │
┌──────────────────┐  ┌────────▼─────────────┐  ┌──────────────────────┐
│   DIM_ACCOUNT    │  │  FACT_TRANSACTIONS   │  │    DIM_MERCHANT      │
│──────────────────│  │──────────────────────│  │──────────────────────│
│ account_id (PK)  ├──┤ transaction_id (PK)  ├──┤ merchant_id (PK)     │
│ customer_age     │  │ account_id (FK)      │  │ location             │
│ occupation       │  │ merchant_id (FK)     │  │ channel              │
│ account_balance  │  │ date_id (FK)         │  └──────────────────────┘
└──────────────────┘  │ type_id (FK)         │
                      │ amount               │  ┌──────────────────────┐
                      │ balance              │  │ DIM_TRANSACTION_TYPE │
                      │ duration_seconds     ├──┤──────────────────────│
                      │ login_attempts       │  │ type_id (PK)         │
                      │ is_suspicious        │  │ transaction_type     │
                      │ transaction_date     │  └──────────────────────┘
                      └──────────────────────┘
```

### Tables Summary

| Table | Rows | Description |
|-------|------|-------------|
| `FACT_TRANSACTIONS` | 11,500 | Core transactions with all metrics |
| `DIM_DATE` | 2,150 | Date dimension with calendar attributes |
| `DIM_ACCOUNT` | 495 | Customer account information |
| `DIM_MERCHANT` | 100 | Merchant/location details |
| `DIM_TRANSACTION_TYPE` | 2 | Transaction type (debit/credit) |

---

## 🔄 Pipeline Steps

### Step 1 — Simulation (`simulate_transactions.py`)
Splits the source CSV into 500-row batches and writes them to the landing zone every 60 seconds, simulating real-time transaction streams.

```python
# Key logic
batch = df.iloc[start_idx : start_idx + BATCH_SIZE]
batch.to_csv(f"{LANDING_ZONE}/transactions_batch_{batch_num}_{ts}.csv")
time.sleep(SLEEP_SECONDS)
```

### Step 2 — Extract (`extract.py`)
Reads all CSV batches from the landing zone and uploads them to HDFS using the **WebHDFS REST API**, partitioned by date.

```python
# Upload via WebHDFS
r = requests.put(f"http://hadoop-namenode:9870/webhdfs/v1/banking/raw/date={today}/{fname}
                   ?op=CREATE&user.name=root&overwrite=true", allow_redirects=False)
requests.put(r.headers["Location"], data=open(f, "rb"))
```

**Why WebHDFS instead of `hdfs` CLI?**
The Airflow container doesn't have the Hadoop CLI installed. WebHDFS is a REST API that works from any container over HTTP — no Hadoop client needed.

### Step 3 — Transform (`spark/transform.py`)
PySpark reads raw CSVs from HDFS, applies cleaning operations, and builds the Star Schema:

- ✅ Drop duplicates
- ✅ Remove nulls in critical columns
- ✅ Parse mixed date formats using `try_to_timestamp`
- ✅ Cast data types correctly
- ✅ Flag suspicious transactions (amount > 10,000 or login_attempts > 3)
- ✅ Build 4 Dimension tables + 1 Fact table
- ✅ Write output as **Parquet** to HDFS staged zone

**Why Parquet?**
Parquet is a columnar format — reading only the columns needed for a query is 10-100x faster than CSV, and files are significantly smaller due to built-in compression.

### Step 4 — Load (`spark/load.py`)
Reads Parquet files from HDFS and loads them into Snowflake using the `snowflake-connector-python` library.

```python
write_pandas(conn, df_pandas, table_name.upper(),
             auto_create_table=False, overwrite=True, quote_identifiers=False)
```

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop installed and running
- A free Snowflake account ([sign up here](https://signup.snowflake.com/))
- The dataset from Kaggle: search for **"banking transactions dataset"**

### 1. Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/banking-etl-pipeline.git
cd banking-etl-pipeline
```

### 2. Add the dataset
```bash
mkdir -p data/raw data/landing data/processed
# Copy your Kaggle CSV here:
cp /path/to/bank_transactions.csv data/raw/
```

### 3. Set up Snowflake
Run `snowflake_setup.sql` in your Snowflake Worksheet to create the database and tables.

### 4. Configure credentials
```bash
cp .env.example .env
nano .env
```

```env
SNOWFLAKE_URL=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DB=BANKING_DWH
SNOWFLAKE_SCHEMA=STAR_SCHEMA
SNOWFLAKE_WH=COMPUTE_WH
```

> ⚠️ **Never commit `.env` to GitHub!** It's already in `.gitignore`.

### 5. Start all services
```bash
docker compose up -d --build
```

Wait 2-3 minutes for all services to become healthy:
```bash
docker compose ps
```

---

## ▶️ Running the Pipeline

### Option A — Automatic (Airflow)
The DAG runs automatically every day at midnight. To trigger manually:

```bash
# Via CLI
docker compose exec airflow-webserver \
  airflow dags trigger banking_etl_pipeline

# Or via Airflow UI at http://localhost:18080
```

### Option B — Manual (step by step)

```bash
# Step 1: Simulate transactions
docker compose exec airflow-scheduler \
  python3 /opt/airflow/scripts/simulate_transactions.py

# Step 2: Extract to HDFS (done automatically by DAG)

# Step 3: Transform with PySpark
docker compose exec airflow-scheduler \
  bash -c "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 && \
           python3 /opt/airflow/spark/transform.py"

# Step 4: Load to Snowflake
docker compose exec airflow-scheduler \
  python3 /opt/airflow/spark/load.py
```

---

## ⏰ Airflow DAG

The DAG `banking_etl_pipeline` consists of 3 tasks running sequentially:

```
extract_to_hdfs  ──►  spark_transform  ──►  load_to_snowflake
```

| Task | Description | Typical Duration |
|------|-------------|-----------------|
| `extract_to_hdfs` | Upload CSV batches to HDFS | ~10 seconds |
| `spark_transform` | PySpark clean + Star Schema | ~3-5 minutes |
| `load_to_snowflake` | Load Parquet → Snowflake | ~1-2 minutes |

**Schedule:** `0 0 * * *` — Daily at midnight

---

## 🌐 Services & Ports

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:18080 | airflow / airflow |
| **HDFS NameNode UI** | http://localhost:9870 | — |
| **YARN ResourceManager UI** | http://localhost:8088 | — |
| **Jupyter Notebook** | http://localhost:8899 | — |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

---

## 📊 Dataset

- **Source:** [Kaggle — Banking Transactions Dataset](https://www.kaggle.com/)
- **Size:** ~11,500 transactions
- **Key Columns:**

| Column | Type | Description |
|--------|------|-------------|
| `TransactionID` | String | Unique transaction identifier |
| `AccountID` | String | Customer account number |
| `TransactionAmount` | Float | Transaction value |
| `TransactionDate` | Timestamp | Date and time of transaction |
| `TransactionType` | String | debit / credit |
| `MerchantID` | String | Merchant identifier |
| `Location` | String | Transaction location |
| `Channel` | String | online / branch / ATM |
| `CustomerAge` | Integer | Customer age |
| `CustomerOccupation` | String | Customer job |
| `LoginAttempts` | Integer | Login attempts before transaction |
| `AccountBalance` | Float | Balance after transaction |

---

## 🔧 Troubleshooting

### Java not found after restart
```bash
docker compose exec --user root airflow-scheduler \
  apt-get install -y default-jdk -qq
```

### Landing zone is empty
```bash
mv data/processed/*.csv data/landing/ 2>/dev/null
```

### Airflow DAG not showing
```bash
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec airflow-webserver airflow dags list
```

### Reset everything
```bash
docker compose down -v
sudo rm -rf data/landing/* data/processed/*
docker compose up -d --build
```

---

## 👤 Author

**Mohamed Kassab**
- GitHub: [@mohamedkasssb](https://github.com/mohamedkasssb)

---

<div align="center">
Made with ❤️ using Apache Spark, Hadoop, Airflow & Snowflake
</div>
