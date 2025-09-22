from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2, requests, random

DB_CONN = "host=postgres dbname=retail_db user=retail_user password=retail_pass port=5432"

def load_products():
    resp = requests.get("https://fakestoreapi.com/products")
    products = resp.json()

    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    for p in products:
        name = p["title"] + (" " if random.random() < 0.2 else "")
        category = p["category"].capitalize() if random.random() < 0.8 else None
        brand = "Unknown" if random.random() < 0.3 else "Brand" + str(random.randint(1, 5))

        cur.execute("""
            INSERT INTO raw.products_catalog(product_id, product_name, category, brand)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
        """, (p["id"], name, category, brand))

    conn.commit()
    cur.close()
    conn.close()

with DAG("products_dag", start_date=datetime(2025, 9, 1), schedule_interval="@once", catchup=False) as dag:
    task = PythonOperator(task_id="load_products", python_callable=load_products)
