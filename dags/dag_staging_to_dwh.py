from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="staging_to_dwh",
    default_args=default_args,
    description="ETL: STAGING → DWH",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "staging", "dwh"],
) as dag:

    load_dim_product = PostgresOperator(
        task_id="load_dim_product",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO dwh.dim_product (product_id, product_name, category, brand)
            SELECT DISTINCT product_id, product_name, category, brand
            FROM staging.products_stg
            ON CONFLICT (product_id) DO NOTHING;
        """,
    )

    load_dim_date = PostgresOperator(
        task_id="load_dim_date",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO dwh.dim_date (date_id, year, month, week, day, is_holiday)
            SELECT DISTINCT s.sale_date,
                   EXTRACT(YEAR FROM s.sale_date),
                   EXTRACT(MONTH FROM s.sale_date),
                   EXTRACT(WEEK FROM s.sale_date),
                   EXTRACT(DAY FROM s.sale_date),
                   FALSE
            FROM staging.sales_stg s
            ON CONFLICT (date_id) DO NOTHING;
        """,
    )

    load_fct_sales = PostgresOperator(
        task_id="load_fct_sales",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO dwh.fct_sales (date_id, store_id, product_id, qty, revenue)
            SELECT sale_date, store_id, product_id, qty, revenue
            FROM staging.sales_stg;
        """,
    )

    load_fct_sales_history = PostgresOperator(
        task_id="load_fct_sales_history",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO dwh.fct_sales_history (date_id, store_id, product_id, qty, revenue)
            SELECT sale_date, store_id, product_id, qty, revenue
            FROM staging.sales_stg;
        """,
    )

    load_fct_competitors = PostgresOperator(
        task_id="load_fct_competitors",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO dwh.fct_competitors (check_date, competitor, product_id, price)
            SELECT check_date, competitor, product_id, price
            FROM staging.competitors_stg;
        """,
    )

    load_fct_currency = PostgresOperator(
        task_id="load_fct_currency",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO dwh.fct_currency (rate_date, currency, rate)
            SELECT rate_date, currency, rate
            FROM staging.currency_stg;
        """,
    )

    load_fct_promotions = PostgresOperator(
        task_id="load_fct_promotions",
        postgres_conn_id="postgres_retail",
        sql="""
            INSERT INTO dwh.fct_promotions (promo_id, product_id, start_date, end_date, discount, description)
            SELECT promo_id, product_id, start_date, end_date, discount, description
            FROM staging.promotions_stg
            ON CONFLICT (promo_id) DO NOTHING;
        """,
    )

    # Исправленная версия зависимостей
    [load_dim_product, load_dim_date] >> load_fct_sales
    [load_dim_product, load_dim_date] >> load_fct_sales_history
    [load_dim_product, load_dim_date] >> load_fct_competitors
    [load_dim_product, load_dim_date] >> load_fct_currency
    [load_dim_product, load_dim_date] >> load_fct_promotions