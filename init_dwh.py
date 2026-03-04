from sqlalchemy import create_engine, text

DWH_DB_URL = 'postgresql://postgres:postgres@localhost:5434/olist_dwh'
engine_dwh = create_engine(DWH_DB_URL)

ddl_queries = """
CREATE SCHEMA IF NOT EXISTS olist_dwh;

-- Hapus tabel lama agar bersih
DROP TABLE IF EXISTS olist_dwh.fact_fulfillment CASCADE;
DROP TABLE IF EXISTS olist_dwh.fact_sales CASCADE;
DROP TABLE IF EXISTS olist_dwh.dim_reviews CASCADE;
DROP TABLE IF EXISTS olist_dwh.dim_date CASCADE;
DROP TABLE IF EXISTS olist_dwh.dim_customers CASCADE;
DROP TABLE IF EXISTS olist_dwh.dim_products CASCADE;
DROP TABLE IF EXISTS olist_dwh.dim_sellers CASCADE;

-- 1. Dimensi Customer (SCD TYPE 2)
CREATE TABLE olist_dwh.dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_date TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- 2. Dimensi Product (SCD TYPE 1)
CREATE TABLE olist_dwh.dim_products (
    product_sk SERIAL PRIMARY KEY,
    product_id TEXT,
    product_category_name TEXT,
    product_weight_g FLOAT
);

-- 3. Dimensi Seller (SCD TYPE 1)
CREATE TABLE olist_dwh.dim_sellers (
    seller_sk SERIAL PRIMARY KEY,
    seller_id TEXT,
    seller_zip_code TEXT,
    seller_city TEXT,
    seller_state TEXT
);

-- 4. Dimensi Review
CREATE TABLE olist_dwh.dim_reviews (
    review_sk SERIAL PRIMARY KEY,
    review_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_creation_date TIMESTAMP
);

-- 5. Dimensi Date (Otomatis Generate 2016-2018)
CREATE TABLE olist_dwh.dim_date (
    date_actual DATE PRIMARY KEY,
    day_name TEXT,
    month_name TEXT,
    year_actual INTEGER,
    quarter_actual INTEGER,
    is_weekend BOOLEAN
);

-- 6. Tabel Fakta Sales
CREATE TABLE olist_dwh.fact_sales (
    sales_pk SERIAL PRIMARY KEY,
    order_id TEXT,
    customer_sk INTEGER REFERENCES olist_dwh.dim_customers(customer_sk),
    product_sk INTEGER REFERENCES olist_dwh.dim_products(product_sk),
    seller_sk INTEGER REFERENCES olist_dwh.dim_sellers(seller_sk),
    price FLOAT,
    freight_value FLOAT,
    order_purchase_timestamp TIMESTAMP
);

-- 7. Tabel Fakta Fulfillment (Poin Penting!)
CREATE TABLE olist_dwh.fact_fulfillment (
    fulfillment_pk SERIAL PRIMARY KEY,
    order_id TEXT,
    customer_sk INTEGER REFERENCES olist_dwh.dim_customers(customer_sk),
    order_purchase_date DATE REFERENCES olist_dwh.dim_date(date_actual),
    shipping_limit_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    delivery_actual_days INTEGER -- Selisih hari pengiriman
);
"""

def setup_dwh():
    print("Membangun Struktur Data Warehouse Lengkap...")
    with engine_dwh.connect() as conn:
        conn.execute(text(ddl_queries))
        
        # Script Tambahan untuk Generate Dim_Date otomatis
        generate_date_sql = """
        INSERT INTO olist_dwh.dim_date
        SELECT
            datum AS date_actual,
            TO_CHAR(datum, 'TMDay') AS day_name,
            TO_CHAR(datum, 'TMMonth') AS month_name,
            EXTRACT(YEAR FROM datum) AS year_actual,
            EXTRACT(QUARTER FROM datum) AS quarter_actual,
            CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
        FROM generate_series('2016-01-01'::DATE, '2019-12-31'::DATE, '1 day'::interval) datum;
        """
        conn.execute(text(generate_date_sql))
        conn.commit()
    print("✅ Semua Tabel (Termasuk Review, Date, & Fulfillment) Sukses Dibuat!")

if __name__ == "__main__":
    setup_dwh()