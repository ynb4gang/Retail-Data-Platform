from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="raw_to_staging",
    default_args=default_args,
    description="ETL: RAW → STAGING",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "raw", "staging"],
) as dag:

    truncate_staging = PostgresOperator(
        task_id="truncate_staging",
        postgres_conn_id="postgres_retail",
        sql="""
            TRUNCATE TABLE staging.sales_stg;
            TRUNCATE TABLE staging.competitors_stg;
            TRUNCATE TABLE staging.currency_stg;
            TRUNCATE TABLE staging.products_stg;
            TRUNCATE TABLE staging.promotions_stg;
        """,
    )

    load_sales = PostgresOperator(
        task_id="load_sales_stg",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO staging.sales_stg (sale_date, store_id, product_id, qty, revenue)
            SELECT DISTINCT date::date, store_id, product_id, qty, revenue
            FROM raw.sales_generated;
        """,
    )

    load_competitors = PostgresOperator(
        task_id="load_competitors_stg",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO staging.competitors_stg (check_date, competitor, product_id, price)
            SELECT DISTINCT date::date, TRIM(competitor), product_id, price
            FROM raw.competitors_api;
        """,
    )

    load_currency = PostgresOperator(
        task_id="load_currency_stg",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO staging.currency_stg (rate_date, currency, rate)
            SELECT DISTINCT date, currency, rate
            FROM raw.currency_rates;
        """,
    )

    load_products = PostgresOperator(
        task_id="load_products_stg",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO staging.products_stg (product_id, product_name, category, brand)
            SELECT DISTINCT product_id, TRIM(product_name), COALESCE(category, 'Unknown'), brand
            FROM raw.products_catalog;
        """,
    )

    load_promotions = PostgresOperator(
        task_id="load_promotions_stg",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO staging.promotions_stg (promo_id, product_id, start_date, end_date, discount, description)
            SELECT DISTINCT promo_id, product_id, start_date, end_date, discount, description
            FROM raw.promotions_api;
        """,
    )

    trigger_dwh = TriggerDagRunOperator(
        task_id="trigger_staging_to_dwh",
        trigger_dag_id="staging_to_dwh",
        trigger_rule="all_success",
    )

    truncate_staging >> [load_sales, load_competitors, load_currency, load_products, load_promotions] >> trigger_dwh
