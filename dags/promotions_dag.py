from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2, requests, random

DB_CONN = "host=postgres dbname=retail_db user=retail_user password=retail_pass port=5432"

def fetch_promotions():
    resp = requests.get("https://fakestoreapi.com/products")
    products = resp.json()

    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    for _ in range(10):
        product = random.choice(products)
        start_date = datetime.now().date() - timedelta(days=random.randint(0, 2))
        end_date = start_date + timedelta(days=random.randint(5, 15))
        discount = round(random.uniform(5, 30), 2)

        description = f"Скидка на {product['title']}"
        if random.random() < 0.3:
            description += " !!!"

        cur.execute("""
            INSERT INTO raw.promotions_api(product_id, start_date, end_date, discount, description)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            product["id"],
            start_date,
            end_date,
            discount,
            description
        ))

    conn.commit()
    cur.close()
    conn.close()

with DAG("promotions_dag", start_date=datetime(2025, 9, 1), schedule_interval="@weekly", catchup=False) as dag:
    task = PythonOperator(task_id="fetch_promotions", python_callable=fetch_promotions)
