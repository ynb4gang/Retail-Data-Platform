from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="manual_full_load",
    default_args=default_args,
    description="Manual one-time run: load raw → staging → dwh",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["manual", "etl"],
) as dag:

    start = DummyOperator(task_id="start")

    # Очистка staging
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

    # Загрузка raw → staging
    load_sales_staging = PostgresOperator(
        task_id="load_sales_staging",
        postgres_conn_id="postgres_retail",
        sql="""
        INSERT INTO staging.sales_stg (sale_date, store_id, product_id, qty, revenue)
        SELECT date, store_id, product_id, qty, revenue
        FROM raw.sales_generated;
        """,
    )

    load_competitors_staging = PostgresOperator(
        task_id="load_competitors_staging",
        postgres_conn_id="postgres_retail",
        sql="""
        INSERT INTO staging.competitors_stg (check_date, competitor, product_id, price)
        SELECT date, competitor, product_id, price
        FROM raw.competitors_api;
        """,
    )

    load_currency_staging = PostgresOperator(
        task_id="load_currency_staging",
        postgres_conn_id="postgres_retail",
        sql="""
        INSERT INTO staging.currency_stg (rate_date, currency, rate)
        SELECT date, currency, rate
        FROM raw.currency_rates;
        """,
    )

    load_products_staging = PostgresOperator(
        task_id="load_products_staging",
        postgres_conn_id="postgres_retail",
        sql="""
        INSERT INTO staging.products_stg (product_id, product_name, category, brand)
        SELECT product_id, product_name, category, brand
        FROM raw.products_catalog;
        """,
    )

    load_promotions_staging = PostgresOperator(
        task_id="load_promotions_staging",
        postgres_conn_id="postgres_retail",
        sql="""
        INSERT INTO staging.promotions_stg (promo_id, product_id, start_date, end_date, discount, description)
        SELECT promo_id, product_id, start_date, end_date, discount, description
        FROM raw.promotions_api;
        """,
    )

    # Загрузка staging → dwh (dimension tables)
    load_dim_product = PostgresOperator(
        task_id="load_dim_product",
        postgres_conn_id="postgres_retail",
        sql="""
        -- Сначала загружаем продукты из каталога
        INSERT INTO dwh.dim_product (product_id, product_name, category, brand)
        SELECT DISTINCT product_id, product_name, category, brand
        FROM staging.products_stg
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            category = EXCLUDED.category,
            brand = EXCLUDED.brand;
            
        -- Затем добавляем ВСЕ продукты из всех источников
        INSERT INTO dwh.dim_product (product_id, product_name, category, brand)
        SELECT DISTINCT product_id, 'Unknown', 'Unknown', 'Unknown'
        FROM (
            SELECT product_id FROM staging.sales_stg
            UNION
            SELECT product_id FROM staging.competitors_stg
            UNION
            SELECT product_id FROM staging.promotions_stg
        ) all_products
        WHERE product_id NOT IN (SELECT product_id FROM dwh.dim_product)
        ON CONFLICT (product_id) DO NOTHING;
        """,
    )

    load_dim_store = PostgresOperator(
        task_id="load_dim_store",
        postgres_conn_id="postgres_retail",
        sql="""
        INSERT INTO dwh.dim_store (store_id, store_name, region)
        SELECT DISTINCT store_id, 'Unknown', 'Unknown'
        FROM staging.sales_stg
        ON CONFLICT (store_id) DO NOTHING;
        """,
    )

    load_dim_date = PostgresOperator(
        task_id="load_dim_date",
        postgres_conn_id="postgres_retail",
        sql="""
        INSERT INTO dwh.dim_date (date_id, year, month, week, day, is_holiday)
        SELECT DISTINCT d::date,
               EXTRACT(YEAR FROM d),
               EXTRACT(MONTH FROM d),
               EXTRACT(WEEK FROM d),
               EXTRACT(DAY FROM d),
               FALSE
        FROM (
            SELECT sale_date AS d FROM staging.sales_stg
            UNION
            SELECT check_date FROM staging.competitors_stg
            UNION
            SELECT rate_date FROM staging.currency_stg
            UNION
            SELECT start_date FROM staging.promotions_stg
            UNION
            SELECT end_date FROM staging.promotions_stg
        ) t
        ON CONFLICT (date_id) DO NOTHING;
        """,
    )

    # Загрузка staging → dwh (fact tables)
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
        INSERT INTO dwh.fct_sales_history (date_id, store_id, product_id, qty, revenue, load_timestamp)
        SELECT sale_date, store_id, product_id, qty, revenue, now()
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
        ON CONFLICT (promo_id) DO UPDATE SET
            product_id = EXCLUDED.product_id,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            discount = EXCLUDED.discount,
            description = EXCLUDED.description;
        """,
    )


    end = DummyOperator(task_id="end")

    # ЗАВИСИМОСТИ
    start >> truncate_staging
    
    # Все загрузки в staging параллельно
    truncate_staging >> [
        load_sales_staging, 
        load_competitors_staging, 
        load_currency_staging, 
        load_products_staging, 
        load_promotions_staging
    ]
    
    # Сначала загружаем все dimension таблицы
    [
        load_sales_staging,
        load_competitors_staging, 
        load_currency_staging,
        load_products_staging,
        load_promotions_staging
    ] >> load_dim_product
    
    load_sales_staging >> load_dim_store
    [
        load_sales_staging,
        load_competitors_staging,
        load_currency_staging,
        load_promotions_staging
    ] >> load_dim_date
    
    # Dimension таблицы должны завершиться перед fact таблицами
    load_dim_product >> [
        load_fct_sales,
        load_fct_sales_history,
        load_fct_competitors,
        load_fct_promotions
    ]
    
    load_dim_store >> [
        load_fct_sales,
        load_fct_sales_history,
        load_fct_competitors
    ]
    
    load_dim_date >> [
        load_fct_sales,
        load_fct_sales_history,
        load_fct_competitors,
        load_fct_currency,
        load_fct_promotions
    ]
    
    [
        load_fct_sales,
        load_fct_sales_history,
        load_fct_competitors,
        load_fct_currency,
        load_fct_promotions
    ] >> end