from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2, requests, random

DB_CONN = "host=postgres dbname=retail_db user=retail_user password=retail_pass port=5432"

def fetch_currency():
    resp = requests.get("https://www.cbr-xml-daily.ru/daily_json.js")
    data = resp.json()

    conn = psycopg2.connect(DB_CONN)
    cur = conn.cursor()

    for curr in ["USD", "EUR", "GBP"]:
        rate = data["Valute"][curr]["Value"]
        if random.random() < 0.3:
            rate = round(rate)

        cur.execute("""
            INSERT INTO raw.currency_rates(date, currency, rate)
            VALUES (CURRENT_DATE, %s, %s)
        """, (curr, rate))

    conn.commit()
    cur.close()
    conn.close()

with DAG("currency_dag", start_date=datetime(2025, 9, 1), schedule_interval="@daily", catchup=False) as dag:
    task = PythonOperator(task_id="fetch_currency", python_callable=fetch_currency)
