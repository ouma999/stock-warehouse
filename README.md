# Stock Data Warehouse

A production-style data engineering project that automatically extracts real stock market data, transforms it with analytics, and loads it into a PostgreSQL data warehouse — all orchestrated with Apache Airflow running in Docker.

---

## 🏗️ Architecture Overview

```
Yahoo Finance API
       │
       ▼
┌─────────────────┐
│   Apache Airflow │  ← Orchestrates the pipeline (schedules, monitors, retries)
│   (Scheduler)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Python ETL     │  ← Extracts, transforms, and loads the data
│  (DAG Tasks)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │  ← Stores the data warehouse (prices, averages, % change)
│   Database      │
└─────────────────┘

All services run inside Docker containers on your local machine.
```

---

## 🛠️ Tech Stack

| Tool | Role | Why We Used It |
|------|------|----------------|
| **Apache Airflow** | Pipeline orchestration | Industry standard for scheduling and monitoring data pipelines. Provides a visual UI, retry logic, and task dependency management |
| **PostgreSQL** | Data warehouse | Reliable, open-source relational database. Perfect for storing structured time-series stock data |
| **Docker** | Containerization | Packages all dependencies (Airflow, PostgreSQL, Redis) into isolated containers so the project runs identically on any machine |
| **Docker Compose** | Multi-container management | Defines and runs all containers together with a single command |
| **Redis** | Message broker | Used internally by Airflow's CeleryExecutor to distribute tasks across workers |
| **yfinance** | Stock data API | Free Python library that fetches real-time and historical stock data from Yahoo Finance |
| **Python** | ETL logic | Powers all data extraction, transformation, and loading tasks |
| **psycopg2** | PostgreSQL connector | Python library for connecting to and querying PostgreSQL |

---

## 📊 What the Pipeline Does

The pipeline runs **daily** and performs 3 tasks in sequence:

```
create_table → extract_stock_data → check_alerts
```

### Task 1: `create_table`
Creates the `stock_prices` table in PostgreSQL if it doesn't exist yet. This is idempotent — safe to run multiple times.

### Task 2: `extract_stock_data`
For each of the 8 tracked stocks:
1. **Extracts** 10 days of historical price data from Yahoo Finance
2. **Transforms** it by calculating:
   - Current closing price
   - Trading volume
   - 7-day moving average
   - Percentage change from previous day
3. **Loads** the result into PostgreSQL

### Task 3: `check_alerts`
Queries the database for any stocks that moved more than 2% in the last hour and logs alerts.

---

## 📁 Project Structure

```
stock-warehouse/
├── dags/
│   └── stock_pipeline.py      # The Airflow DAG (pipeline definition)
├── logs/                      # Airflow task logs (auto-generated)
├── plugins/                   # Custom Airflow plugins (empty for now)
├── docker-compose.yaml        # Defines all Docker services
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables (AIRFLOW_UID)
└── README.md                  # This file
```

---

## 🚀 Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Git

### Step 1: Clone the repository
```bash
git clone https://github.com/YOUR_USERNAME/stock-warehouse.git
cd stock-warehouse
```

### Step 2: Set up environment
```bash
mkdir -p ./logs ./plugins
echo "AIRFLOW_UID=$(id -u)" > .env
```

### Step 3: Initialize Airflow
```bash
docker compose up airflow-init
```
This sets up the Airflow metadata database and creates the default admin user.

### Step 4: Start all services
```bash
docker compose up -d
```
This starts 5 containers:
- `airflow-webserver` — the Airflow UI (port 8080)
- `airflow-scheduler` — monitors and triggers DAGs
- `airflow-worker` — executes the tasks
- `postgres` — stores both Airflow metadata and your stock data
- `redis` — message broker between scheduler and workers

### Step 5: Install dependencies
```bash
docker compose exec airflow-webserver bash -c "pip install yfinance --no-cache-dir"
docker compose exec airflow-scheduler bash -c "pip install yfinance --no-cache-dir"
docker compose exec airflow-worker bash -c "pip install yfinance --no-cache-dir"
```

### Step 6: Access the Airflow UI
Open your browser and go to: **http://localhost:8080**

- **Username:** `airflow`
- **Password:** `airflow`

### Step 7: Run the pipeline
1. Search for `stock_data_pipeline` in the DAGs list
2. Toggle it on (unpause)
3. Click the ▶ Play button → **Trigger DAG**
4. Watch the tasks turn green!

### Step 8: Query the data
```bash
docker compose exec postgres psql -U airflow -c "
SELECT ticker, 
       ROUND(price::numeric, 2) as price, 
       ROUND(moving_avg_7d::numeric, 2) as avg_7d, 
       ROUND(pct_change::numeric, 2) as pct_change 
FROM stock_prices 
ORDER BY ticker;
"
```

---

## 🔍 How Docker Works in This Project

Docker solves the "it works on my machine" problem by packaging software into **containers** — isolated environments with everything the app needs.

```
Without Docker:                    With Docker:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Install Python 3.12             docker compose up ✅
2. Install PostgreSQL 13
3. Install Redis
4. pip install airflow (100+ deps)
5. Configure connections
6. Hope nothing conflicts
```

**Docker Compose** manages multiple containers together. Our `docker-compose.yaml` defines:
- Which images to use
- How containers connect to each other
- Which ports to expose
- Shared volumes (like the `dags/` folder)

---

## 🌊 How Airflow Works

Airflow organizes pipelines as **DAGs** (Directed Acyclic Graphs):

```
create_table ──► extract_stock_data ──► check_alerts
```

- **DAG** = the whole pipeline
- **Task** = a single step (e.g. `extract_stock_data`)
- **Operator** = the type of task (`PythonOperator` runs Python functions)
- **Schedule** = when to run (`timedelta(days=1)` = every 24 hours)

The Airflow **Scheduler** constantly checks if any DAGs are due to run. When they are, it sends tasks to the **Worker** via **Redis**. The **Webserver** shows you everything in the UI.

---

## 📈 Tracked Stocks

| Ticker | Company |
|--------|---------|
| AAPL | Apple |
| GOOGL | Alphabet (Google) |
| MSFT | Microsoft |
| TSLA | Tesla |
| AMZN | Amazon |
| NVDA | NVIDIA |
| META | Meta (Facebook) |
| NFLX | Netflix |

---

## 🗄️ Database Schema

```sql
CREATE TABLE stock_prices (
    id           SERIAL PRIMARY KEY,
    ticker       VARCHAR(10),        -- Stock symbol (e.g. AAPL)
    price        FLOAT,              -- Closing price
    volume       BIGINT,             -- Trading volume
    date         DATE,               -- Trading date
    moving_avg_7d FLOAT,             -- 7-day moving average
    pct_change   FLOAT,              -- % change from previous day
    created_at   TIMESTAMP           -- When the record was inserted
);
```

---

## 🔮 Future Improvements

- [ ] Add a Grafana dashboard for visualization
- [ ] Store more historical data (1 year)
- [ ] Add email/Slack alerts for big price movements
- [ ] Add more technical indicators (RSI, MACD, Bollinger Bands)
- [ ] Deploy to cloud (AWS/GCP)
- [ ] Add unit tests

---

## 👤 Author

Built by **ouma999**
