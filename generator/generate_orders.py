import os
import time
import random
import uuid
from datetime import datetime, timezone

import psycopg2

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "sales")
DB_USER = os.getenv("DB_USER", "sales")
DB_PASSWORD = os.getenv("DB_PASSWORD", "sales")
RATE_SECONDS = float(os.getenv("RATE_SECONDS", "10"))

CATEGORIES = ["Shoes", "Jackets", "Gadgets", "Home", "Clothing", "Accessories"]
CHANNELS = ["web", "mobile", "store"]
COUNTRIES = ["DE", "PK", "US", "GB", "FR", "NL"]

PRODUCTS = {
    "Shoes": ["p001", "p002", "p003", "p004", "p005"],
    "Jackets": ["p006", "p007", "p008", "p009", "p010"],
    "Gadgets": ["p011", "p012", "p013", "p014", "p015"],
    "Home": ["p016", "p017", "p018", "p019", "p020"],
    "Clothing": ["p021", "p022", "p023", "p024", "p025"],
    "Accessories": ["p026", "p027", "p028", "p029", "p030"],
}

def connect():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def insert_order(conn):
    category = random.choice(CATEGORIES)
    product_id = random.choice(PRODUCTS[category])
    channel = random.choice(CHANNELS)
    country = random.choice(COUNTRIES)
    qty = random.randint(1, 4)
    unit_price = round(random.uniform(8, 200), 2)

    order_id = str(uuid.uuid4())[:8]
    customer_id = f"c{random.randint(1, 500):04d}"
    created_at = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO source.orders_raw
            (order_id, created_at, customer_id, product_id, category, channel, country, qty, unit_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (order_id, created_at, customer_id, product_id, category, channel, country, qty, unit_price),
        )
    conn.commit()

def main():
    # Wait for DB
    while True:
        try:
            conn = connect()
            conn.close()
            break
        except Exception:
            time.sleep(2)

    conn = connect()
    print("Order generator started.")

    try:
        while True:
            insert_order(conn)
            time.sleep(RATE_SECONDS)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
