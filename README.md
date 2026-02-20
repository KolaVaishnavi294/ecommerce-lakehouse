# 🛒 Ecommerce Lakehouse with Spark + Delta Lake + MinIO (Dockerized)
## Project Overview

This project implements a Modern Data Lakehouse Architecture using Apache Spark, Delta Lake, and MinIO (S3-compatible storage), fully containerized with Docker.

The system performs:

- ✅ Batch ingestion

- ✅ Streaming ingestion

- ✅ Delta Lake upsert (MERGE)

- ✅ ACID-compliant storage

- ✅ Checkpoint-based fault tolerance

All processed data is stored as Delta tables inside MinIO object storage.

---

## Architecture
```bash
CSV Files  →  Spark (Master + Worker)
                ↓
           Delta Lake
                ↓
            MinIO (S3 Storage)
```
### System Components:

| Component      | Role                                  |
| -------------- | ------------------------------------- |
| Spark Master   | Cluster coordinator                   |
| Spark Worker   | Executes distributed tasks            |
| Spark App      | Runs ingestion & transformation logic |
| Delta Lake     | ACID storage layer                    |
| MinIO          | Object storage backend                |
| Docker Compose | Container orchestration               |

---

## Project Structure
```bash
ecommerce-lakehouse/
│
├── app/
│   └── main.py
│
├── data/
│   ├── customers.csv
│   ├── products.csv
│   ├── sales.csv
│   └── updates.csv
│
├── .env.example
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Features

- Batch ingestion of Products and Customers

- Streaming ingestion of Sales data

- Delta Lake MERGE (Upsert operation)

- Partitioned Delta tables

- Streaming checkpointing

- S3-compatible storage integration (MinIO)

- Fully Dockerized Spark cluster

---

## Technologies Used

- Python

- Apache Spark 3.5.1

- Delta Lake 3.1.0

- MinIO (S3 compatible storage)

- Docker & Docker Compose

---

## Setup Instructions
### 1️⃣ Clone the Repository
```bash
git clone https://github.com/KolaVaishnavi294/ecommerce-lakehouse
cd ecommerce-lakehouse
```
### 2️⃣ Start the System
```bash
docker-compose up --build
```

This will start:

- Spark Master (Port 8080)

- Spark Worker

- MinIO (Ports 9000 & 9001)

- Spark Application

---

## Access Services
### 🔹 Spark UI
```bash
http://localhost:8080
```

### 🔹 MinIO Console
```bash
http://localhost:9001
```

Username: minioadmin

Password: minioadmin

---

## Data Processing Flow
### 🔹 Step 1 – Batch Load

- Reads products.csv and customers.csv

- Writes them as Delta tables

- Products table partitioned by category

### 🔹 Step 2 – Delta MERGE (Upsert)

- Applies updates from updates.csv

- Uses Delta Lake MERGE INTO

- Demonstrates ACID transaction handling

### 🔹 Step 3 – Streaming Ingestion

- Reads sales.csv as streaming source

- Writes to Delta table

- Stores checkpoint in MinIO

---

## MinIO Bucket Structure
```bash
data/
└── warehouse/
    ├── customers/
    ├── products/
    ├── sales/
    └── checkpoints/
```

Each table contains:

- _delta_log (transaction logs)

- Parquet data files

- Versioned commits

---

## Why Delta Lake?

Delta Lake enhances data lakes with:

- ACID Transactions

- Schema Enforcement

- Time Travel

- MERGE (Upsert) Support

- Fault Tolerance with Checkpointing

- Optimistic Concurrency Control

---

## Sample Streaming Output

The system processes sales data and commits:

Batch 0:
- 17 input rows
- 4 output files
- Delta commit version 0

---

## How to Rerun the Job

Stop containers:
```bash
docker-compose down -v
```

Start fresh:
```bash
docker-compose up --build
```

---

## Learning Outcomes

This project demonstrates:

- Building a complete Lakehouse architecture

- Integrating Spark with S3 storage

- Managing batch + streaming pipelines

- Using Delta Lake for reliable data engineering

- Deploying big data systems with Docker