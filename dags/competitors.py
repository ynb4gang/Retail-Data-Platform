from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2, requests, random

DB_CONN = "host=postgres dbname=retail_db user=retail_user password=retail_pass port=5432"

def fetch_competitors():
    resp = requests.get("https://fakestoreapi.com/products")
    products = resp.json()
    competitors = ["StoreX", "StoreY", "StoreZ"]

    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    for _ in range(20):
        product = random.choice(products)
        competitor = random.choice(competitors)
        base_price = product["price"]
        price = round(base_price * random.uniform(0.5, 1.5), 2)

        cur.execute("""
            INSERT INTO raw.competitors_api(date, competitor, product_id, price)
            VALUES (NOW(), %s, %s, %s)
        """, (
            competitor + (" " if random.random() < 0.2 else ""),
            product["id"],
            price
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG("competitors_dag", start_date=datetime(2025, 9, 1), schedule_interval="@daily", catchup=False) as dag:
    task = PythonOperator(task_id="fetch_competitors", python_callable=fetch_competitors)
