CREATE SCHEMA IF NOT EXISTS source;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw incoming events
CREATE TABLE IF NOT EXISTS source.orders_raw (
  id BIGSERIAL PRIMARY KEY,
  order_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  customer_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  category TEXT NOT NULL,
  channel TEXT NOT NULL,
  country TEXT NOT NULL,
  qty INT NOT NULL,
  unit_price NUMERIC(10,2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_raw_created_at ON source.orders_raw(created_at);

-- Cleaned facts
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
  order_id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL,
  customer_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  category TEXT NOT NULL,
  channel TEXT NOT NULL,
  country TEXT NOT NULL,
  qty INT NOT NULL,
  unit_price NUMERIC(10,2) NOT NULL,
  revenue NUMERIC(12,2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_created_at ON analytics.fact_orders(created_at);
CREATE INDEX IF NOT EXISTS idx_fact_orders_category ON analytics.fact_orders(category);

-- Minute-level revenue (near real-time trend)
CREATE TABLE IF NOT EXISTS analytics.agg_sales_minute (
  minute_bucket TIMESTAMPTZ PRIMARY KEY,
  orders_count INT NOT NULL,
  revenue NUMERIC(12,2) NOT NULL
);

-- Daily top products
CREATE TABLE IF NOT EXISTS analytics.agg_top_products_daily (
  day DATE NOT NULL,
  product_id TEXT NOT NULL,
  orders_count INT NOT NULL,
  revenue NUMERIC(12,2) NOT NULL,
  PRIMARY KEY (day, product_id)
);

-- Watermark for incremental loads
CREATE TABLE IF NOT EXISTS analytics.etl_watermark (
  pipeline TEXT PRIMARY KEY,
  last_processed_at TIMESTAMPTZ NOT NULL
);

INSERT INTO analytics.etl_watermark (pipeline, last_processed_at)
VALUES ('sales_etl', '1970-01-01T00:00:00Z')
ON CONFLICT (pipeline) DO NOTHING;
