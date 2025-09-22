-- =====================================
-- 1. Создание схем
-- =====================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;

-- =====================================
-- 2. RAW слой (сырые данные)
-- =====================================

-- Продажи (автогенерация)
CREATE TABLE IF NOT EXISTS raw.sales_generated (
    date TIMESTAMP,
    store_id INT,
    product_id INT,
    qty INT,
    revenue NUMERIC
);

-- Цены конкурентов (API / парсинг)
CREATE TABLE IF NOT EXISTS raw.competitors_api (
    date TIMESTAMP,
    competitor TEXT,
    product_id INT,
    price NUMERIC
);

-- Курсы валют (API)
CREATE TABLE IF NOT EXISTS raw.currency_rates (
    date DATE,
    currency VARCHAR(10),
    rate NUMERIC
);

-- Каталог продуктов (справочник)
CREATE TABLE IF NOT EXISTS raw.products_catalog (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    brand TEXT
);

-- Акции и скидки (API/CSV)
CREATE TABLE IF NOT EXISTS raw.promotions_api (
    promo_id SERIAL PRIMARY KEY,
    product_id INT,
    start_date DATE,
    end_date DATE,
    discount NUMERIC,   -- % скидки
    description TEXT
);

-- =====================================
-- 3. STAGING слой (очистка и нормализация)
-- =====================================

CREATE TABLE IF NOT EXISTS staging.sales_stg (
    sale_date DATE,
    store_id INT,
    product_id INT,
    qty INT,
    revenue NUMERIC
);

CREATE TABLE IF NOT EXISTS staging.competitors_stg (
    check_date DATE,
    competitor TEXT,
    product_id INT,
    price NUMERIC
);

CREATE TABLE IF NOT EXISTS staging.currency_stg (
    rate_date DATE,
    currency VARCHAR(10),
    rate NUMERIC
);

CREATE TABLE IF NOT EXISTS staging.products_stg (
    product_id INT,
    product_name TEXT,
    category TEXT,
    brand TEXT
);

CREATE TABLE IF NOT EXISTS staging.promotions_stg (
    promo_id INT,
    product_id INT,
    start_date DATE,
    end_date DATE,
    discount NUMERIC,
    description TEXT
);

-- =====================================
-- 4. DWH слой (звезда: факты + измерения)
-- =====================================

-- Размерность: продукты
CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id INT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    brand TEXT
);

-- Размерность: магазины
CREATE TABLE IF NOT EXISTS dwh.dim_store (
    store_id INT PRIMARY KEY,
    store_name TEXT,
    region TEXT
);

-- Размерность: даты
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_id DATE PRIMARY KEY,
    year INT,
    month INT,
    week INT,
    day INT,
    is_holiday BOOLEAN
);

-- Факт: продажи (текущие)
CREATE TABLE IF NOT EXISTS dwh.fct_sales (
    date_id DATE REFERENCES dwh.dim_date(date_id),
    store_id INT REFERENCES dwh.dim_store(store_id),
    product_id INT REFERENCES dwh.dim_product(product_id),
    qty INT,
    revenue NUMERIC
);

-- Факт: продажи (историчность)
CREATE TABLE IF NOT EXISTS dwh.fct_sales_history (
    history_id SERIAL PRIMARY KEY,
    date_id DATE REFERENCES dwh.dim_date(date_id),
    store_id INT REFERENCES dwh.dim_store(store_id),
    product_id INT REFERENCES dwh.dim_product(product_id),
    qty INT,
    revenue NUMERIC,
    load_timestamp TIMESTAMP DEFAULT now()
);

-- Факт: курсы валют
CREATE TABLE IF NOT EXISTS dwh.fct_currency (
    rate_date DATE REFERENCES dwh.dim_date(date_id),
    currency VARCHAR(10),
    rate NUMERIC
);

-- Факт: конкуренты (цены)
CREATE TABLE IF NOT EXISTS dwh.fct_competitors (
    check_date DATE REFERENCES dwh.dim_date(date_id),
    competitor TEXT,
    product_id INT REFERENCES dwh.dim_product(product_id),
    price NUMERIC
);

-- Факт: акции/скидки
CREATE TABLE IF NOT EXISTS dwh.fct_promotions (
    promo_id INT PRIMARY KEY,
    product_id INT REFERENCES dwh.dim_product(product_id),
    start_date DATE,
    end_date DATE,
    discount NUMERIC,
    description TEXT
);
