from __future__ import annotations

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
PIPELINE = "sales_etl"
CONN_ID = "sales_postgres"

def run_etl():
    hook = PostgresHook(postgres_conn_id=CONN_ID)

    with hook.get_conn() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # Get watermark
            cur.execute(
                "SELECT last_processed_at FROM analytics.etl_watermark WHERE pipeline = %s",
                (PIPELINE,),
            )
            last_ts = cur.fetchone()[0]

            # Load new raw orders into fact_orders (upsert by order_id)
            cur.execute(
                """
                INSERT INTO analytics.fact_orders
                (order_id, created_at, customer_id, product_id, category, channel, country, qty, unit_price, revenue)
                SELECT
                  r.order_id,
                  r.created_at,
                  r.customer_id,
                  r.product_id,
                  r.category,
                  r.channel,
                  r.country,
                  r.qty,
                  r.unit_price,
                  (r.qty * r.unit_price)::numeric(12,2) AS revenue
                FROM source.orders_raw r
                WHERE r.created_at > %s
                ON CONFLICT (order_id)
                DO UPDATE SET
                  created_at = EXCLUDED.created_at,
                  customer_id = EXCLUDED.customer_id,
                  product_id = EXCLUDED.product_id,
                  category = EXCLUDED.category,
                  channel = EXCLUDED.channel,
                  country = EXCLUDED.country,
                  qty = EXCLUDED.qty,
                  unit_price = EXCLUDED.unit_price,
                  revenue = EXCLUDED.revenue
                """,
                (last_ts,),
            )

            # Update aggregates for last 2 hours (fast + "real-time" feel)
            cur.execute(
                """
                WITH recent AS (
                  SELECT *
                  FROM analytics.fact_orders
                  WHERE created_at >= NOW() - INTERVAL '2 hours'
                ),
                by_minute AS (
                  SELECT
                    date_trunc('minute', created_at) AS minute_bucket,
                    COUNT(*) AS orders_count,
                    SUM(revenue)::numeric(12,2) AS revenue
                  FROM recent
                  GROUP BY 1
                )
                INSERT INTO analytics.agg_sales_minute (minute_bucket, orders_count, revenue)
                SELECT minute_bucket, orders_count, revenue
                FROM by_minute
                ON CONFLICT (minute_bucket)
                DO UPDATE SET
                  orders_count = EXCLUDED.orders_count,
                  revenue = EXCLUDED.revenue
                """
            )

            # Update top products for today (simple but impressive)
            cur.execute(
                """
                WITH today AS (
                  SELECT
                    created_at::date AS day,
                    product_id,
                    COUNT(*) AS orders_count,
                    SUM(revenue)::numeric(12,2) AS revenue
                  FROM analytics.fact_orders
                  WHERE created_at::date = CURRENT_DATE
                  GROUP BY 1, 2
                )
                INSERT INTO analytics.agg_top_products_daily (day, product_id, orders_count, revenue)
                SELECT day, product_id, orders_count, revenue
                FROM today
                ON CONFLICT (day, product_id)
                DO UPDATE SET
                  orders_count = EXCLUDED.orders_count,
                  revenue = EXCLUDED.revenue
                """
            )

            # Advance watermark to max raw created_at
            cur.execute("SELECT COALESCE(MAX(created_at), %s) FROM source.orders_raw", (last_ts,))
            new_last_ts = cur.fetchone()[0]

            cur.execute(
                """
                UPDATE analytics.etl_watermark
                SET last_processed_at = %s
                WHERE pipeline = %s
                """,
                (new_last_ts, PIPELINE),
            )

        conn.commit()

with DAG(
    dag_id="sales_realtime_etl",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="*/1 * * * *",  # every minute
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["portfolio", "sales", "etl"],
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )

    etl_task
