from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2, requests, random

DB_CONN = "host=postgres dbname=retail_db user=retail_user password=retail_pass port=5432"

def generate_sales():
    resp = requests.get("https://fakestoreapi.com/products")
    products = resp.json()

    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    for _ in range(50):
        product = random.choice(products)
        qty = random.randint(1, 5)
        price = product["price"] + random.uniform(-2, 5)
        revenue = round(qty * price, 2)

        cur.execute("""
            INSERT INTO raw.sales_generated(date, store_id, product_id, qty, revenue)
            VALUES (NOW(), %s, %s, %s, %s)
        """, (
            random.randint(1, 5),
            product["id"],
            qty,
            revenue
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG("sales_dag", start_date=datetime(2025, 9, 1), schedule_interval="@daily", catchup=False) as dag:
    task = PythonOperator(task_id="generate_sales", python_callable=generate_sales)
