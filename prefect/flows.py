from prefect import flow, task
import requests
import os
import psycopg2

@task
def fetch_data(url: str):
    r = requests.get(url)
    r.raise_for_status()
    return r.text

@task
def write_to_postgres(data: str):
    conn = psycopg2.connect(host='postgres', dbname=os.getenv('POSTGRES_DB'), user=os.getenv('POSTGRES_USER'), password=os.getenv('POSTGRES_PASSWORD'))
    cur = conn.cursor()
    cur.execute('INSERT INTO raw.raw_sales (source, data) VALUES (%s, %s)', ('prefect_csv', data))
    conn.commit()
    cur.close()
    conn.close()

@flow
def prefect_scraper_flow():
    data = fetch_data('https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv')
    write_to_postgres(data)

if __name__ == '__main__':
    prefect_scraper_flow()