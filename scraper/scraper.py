import os
import requests
import csv
import io
import psycopg2
from dotenv import load_dotenv

load_dotenv()
PG_DSN = {
    'host': 'postgres',
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

CSV_URL = 'https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv'  # тестовый CSV


def pg_conn():
    return psycopg2.connect(host=PG_DSN['host'], dbname=PG_DSN['dbname'], user=PG_DSN['user'], password=PG_DSN['password'])


def fetch_and_write_raw():
    r = requests.get(CSV_URL, timeout=15)
    r.raise_for_status()
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO raw.raw_sales (source, data) VALUES (%s, %s)", ('test_csv', r.text))
    conn.commit()
    cur.close()
    conn.close()
    print('Inserted raw CSV')


def parse_and_insert_staging():
    conn = pg_conn()
    cur = conn.cursor()
    r = requests.get(CSV_URL, timeout=15)
    r.raise_for_status()
    f = io.StringIO(r.text)
    reader = csv.reader(f)
    rows = list(reader)
    # пропускаем заголовки и вставляем демонстрационно
    for i, row in enumerate(rows[1:6]):
        cur.execute("INSERT INTO staging.sales (sale_date, sku, store_id, qty, price) VALUES (%s,%s,%s,%s,%s)", ('2025-01-01', f'sku_{i}', 'store_1', 1+i, 9.99))
    conn.commit()
    cur.close()
    conn.close()
    print('Inserted staging rows')


if __name__ == '__main__':
    fetch_and_write_raw()
    parse_and_insert_staging()