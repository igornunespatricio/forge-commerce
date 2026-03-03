# 📦 E-Commerce Data Warehouse at Scale (Faker + Spark + Airflow)

## 📌 Project Overview

This project simulates a **large-scale e-commerce data platform** using **synthetic data generated with Faker** and processes it using **distributed data engineering technologies**.

The goal is to design and implement an **end-to-end batch data pipeline** that:

* Generates tens of millions of realistic records
* Stores raw data in a data lake
* Transforms data using distributed processing
* Models analytics-ready warehouse tables
* Orchestrates workflows reliably

This project is designed to closely resemble **real-world data engineering workloads**.

---

## 🎯 Objectives

* Generate **large-scale fake data** safely using Faker
* Build **ETL pipelines** using Spark
* Implement **data modeling** (star schema)
* Use **Parquet** for efficient storage
* Orchestrate workflows with Airflow
* Apply **data quality checks**
* Make the data **analytics-ready**

---

## 🏗️ Architecture

```text
Faker (Python)
     ↓
Raw Data (CSV / JSON)
     ↓
Data Lake (raw → cleaned → curated)
     ↓
Apache Spark (ETL & transformations)
     ↓
Parquet Tables (Partitioned)
     ↓
Data Warehouse (Star Schema)
     ↓
Analytics / BI Queries
```

---

## 🧰 Tech Stack

* **Python** – data generation & orchestration
* **Faker** – synthetic data generation
* **Apache Spark** – distributed processing
* **Apache Airflow** – workflow orchestration
* **Parquet** – columnar storage
* **S3 / HDFS / Local FS** – data lake storage
* **SQL** – analytics queries

---

## 📂 Project Structure

```text
ecommerce-data-platform/
│
├── data/
│   ├── raw/
│   ├── cleaned/
│   └── curated/
│
├── faker/
│   ├── generate_customers.py
│   ├── generate_products.py
│   ├── generate_orders.py
│   └── generate_payments.py
│
├── spark/
│   ├── clean_customers.py
│   ├── transform_orders.py
│   ├── build_fact_orders.py
│   └── build_dimensions.py
│
├── airflow/
│   └── dags/
│       └── ecommerce_etl_dag.py
│
├── sql/
│   └── analytics_queries.sql
│
├── README.md
└── requirements.txt
```

---

## 🧪 Data Model

### Dimension Tables

* `dim_customer`
* `dim_product`
* `dim_date`

### Fact Tables

* `fact_orders`
* `fact_payments`

**Schema type:** Star schema
**Partitioning:** `order_date`, `country`

---

## 📊 Data Volume Targets

| Entity      | Rows (Target) |
| ----------- | ------------- |
| Customers   | 5M–10M        |
| Products    | 500K–1M       |
| Orders      | 20M–50M       |
| Order Items | 50M–100M      |
| Payments    | 20M–50M       |

---

## 🔄 Pipeline Stages

### 1️⃣ Data Generation

* Generate data incrementally (batch-by-batch)
* Introduce realistic skew (popular products, repeat users)
* Write output as CSV or JSON

### 2️⃣ Ingestion

* Load raw data into the data lake
* Preserve schema and metadata

### 3️⃣ Transformation

* Schema validation
* Deduplication
* Type casting
* Joins across large tables
* Aggregations

### 4️⃣ Data Modeling

* Build fact & dimension tables
* Enforce surrogate keys
* Partition data for performance

### 5️⃣ Orchestration

* Schedule pipelines with Airflow
* Handle retries & failures
* Add basic data quality checks

---

## ✅ Data Quality Checks

* Null checks on primary keys
* Duplicate detection
* Referential integrity
* Volume checks (row count thresholds)

---

## 📈 Example Analytics Queries

* Daily revenue
* Top products by revenue
* Customer lifetime value (LTV)
* Orders by country
* Payment success rate

---

## 🚀 How to Run (Local)

```bash
pip install -r requirements.txt
python faker/generate_customers.py
spark-submit spark/transform_orders.py
```

---

## 📌 Future Enhancements

* Add Kafka for real-time order ingestion
* Implement incremental loads (MERGE strategy)
* Add dbt for transformations
* Add monitoring & alerting
* Deploy on cloud (AWS / GCP)

## 📜 License

MIT
