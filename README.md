# Real-Time Sales Dashboard (Airflow + Postgres + Metabase)

An end-to-end, dockerized **real-time analytics** project that simulates e-commerce orders, processes them via an **Airflow ETL pipeline**, and visualizes KPIs in **Metabase**.

**What you get**
- A streaming-like order generator (Python) writing raw events into Postgres
- An Airflow DAG scheduled every minute to run **incremental ETL** (watermark-based)
- Analytics-ready tables (`fact_orders` + aggregates)
- 3 Metabase dashboards: **Sales Overview**, **Top Products**, **Ops & Data Quality**

---

## Architecture

**Flow**
1. **Order Generator (Python)** → inserts new rows into `source.orders_raw`
2. **Airflow DAG** (`sales_realtime_etl`, runs every minute)  
   - reads new raw rows since last watermark  
   - transforms into `analytics.fact_orders`  
   - updates aggregates for fast dashboard queries  
   - updates `analytics.etl_watermark`
3. **Metabase** connects to Postgres and renders dashboards

**Core Tables**
- `source.orders_raw` — raw order events (append-only)
- `analytics.fact_orders` — cleaned, analytics-ready fact table
- `analytics.agg_sales_minute` — minute-level revenue series
- `analytics.agg_top_products_daily` — daily top products
- `analytics.etl_watermark` — incremental processing checkpoint

---

## Dashboards

### 1) Sales Overview
- Revenue / Orders / AOV (latest available day)
- Revenue trend (minute-level)
- Revenue by category
- Latest orders table

### 2) Top Products
- Top 10 products by revenue
- Top 10 products by order count

### 3) Ops & Data Quality
- Watermark timestamp
- Processed vs. unprocessed checks
- Fact table health metrics

---

## Tech Stack

- **Python** (order generation + ETL logic)
- **Apache Airflow** (orchestration, scheduling, retries)
- **PostgreSQL** (raw + analytics layer)
- **Metabase** (dashboards)
- **Docker / Docker Compose** (one-command local setup)

---

## Quick Start

### Prerequisites
- Docker + Docker Compose

### Run everything
```bash
docker compose up --build

```
Services & URLs

Airflow UI: http://localhost:8080
Metabase UI: http://localhost:3000
Postgres: internal container network

Step-by-Step Setup
1) Verify containers are running
```bash
docker compose ps

```

## Services

After `docker compose up --build`, you should see services like:
- `sales-db`
- `airflow-webserver`, `airflow-scheduler`
- `metabase`
- `order-generator`

---

## Airflow (confirm the ETL is running)

1. Open: http://localhost:8080  
2. Find the DAG: `sales_realtime_etl`  
3. Ensure the DAG toggle is **ON**  
4. Trigger a run manually (optional) using ▶️  
5. The schedule should show something like **every minute**

---

## Metabase (connect to Postgres)

1. Open: http://localhost:3000  
2. Add a database → **PostgreSQL**  
3. Use the connection values
4. Click **Save**
5. Run **Sync schema** (Admin → Databases → Sync)

