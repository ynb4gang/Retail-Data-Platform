CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.raw_sales (
    id serial PRIMARY KEY,
    source varchar(255),
    data text,
    created_at timestamptz DEFAULT now()
);

-- простая staging-таблица для разобранных строк
CREATE TABLE IF NOT EXISTS staging.sales (
    id serial PRIMARY KEY,
    sale_date date,
    sku varchar(128),
    store_id varchar(64),
    qty int,
    price numeric(12,2),
    created_at timestamptz DEFAULT now()
);