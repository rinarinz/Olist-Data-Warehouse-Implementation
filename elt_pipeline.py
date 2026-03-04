import pandas as pd
from sqlalchemy import create_engine, text

SRC_DB_URL = 'postgresql://postgres:postgres@localhost:5433/olist_src'
DWH_DB_URL = 'postgresql://postgres:postgres@localhost:5434/olist_dwh'
engine_src = create_engine(SRC_DB_URL)
engine_dwh = create_engine(DWH_DB_URL)

def run_elt():
    # --- MULAI BLOK TRY (PENGAMAN) ---
    try:
        print("🚀 Memulai proses ELT Skala Besar...")

        # FASE 1 : EXTRACT & LOAD KE STAGING
        with engine_dwh.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
            conn.commit()

        tables = {
            'customers': 'olist_src.customers',
            'products': 'olist_src.products',
            'orders': 'olist_src.orders',
            'order_reviews': 'olist_src.order_reviews'
        }

        for t_name, src_path in tables.items():
            df = pd.read_sql(f"SELECT * FROM {src_path}", engine_src)
            df.to_sql(f'stg_{t_name}', engine_dwh, schema='staging', if_exists='replace', index=False)
            print(f"✅ Data {t_name} berhasil di-staging.")

        # ==========================================
        # FASE 2 : TRANSFORM (SQL LOGIC UNTUK SCD & FACT)
        # ==========================================
        print("\n[Transform] Menjalankan logika SCD dan Fact Table...")

        combined_transform_sql = """
        -- A. SCD TYPE 2 UNTUK DIM_CUSTOMERS
        UPDATE olist_dwh.dim_customers dim
        SET expiry_date = CURRENT_TIMESTAMP, 
            is_current = FALSE
        FROM staging.stg_customers stg
        WHERE dim.customer_unique_id = stg.customer_unique_id
          AND dim.is_current = TRUE
          AND (dim.customer_city != stg.customer_city OR dim.customer_state != stg.customer_state);

        INSERT INTO olist_dwh.dim_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
        SELECT DISTINCT ON (stg.customer_unique_id) 
            stg.customer_id, stg.customer_unique_id, stg.customer_zip_code_prefix, stg.customer_city, stg.customer_state
        FROM staging.stg_customers stg
        LEFT JOIN olist_dwh.dim_customers dim 
          ON stg.customer_unique_id = dim.customer_unique_id AND dim.is_current = TRUE
        WHERE dim.customer_unique_id IS NULL 
           OR (dim.customer_city != stg.customer_city OR dim.customer_state != stg.customer_state);

        -- B. MENGISI FACT_FULFILLMENT
        TRUNCATE TABLE olist_dwh.fact_fulfillment;

        INSERT INTO olist_dwh.fact_fulfillment (
            order_id, customer_sk, order_purchase_date, shipping_limit_date, 
            order_delivered_customer_date, delivery_actual_days
        )
        SELECT 
            stg_o.order_id,
            dim.customer_sk,
            CAST(stg_o.order_purchase_timestamp AS DATE),
            CAST(stg_o.order_purchase_timestamp AS TIMESTAMP),
            CAST(stg_o.order_delivered_customer_date AS TIMESTAMP),
            EXTRACT(DAY FROM (CAST(stg_o.order_delivered_customer_date AS TIMESTAMP) - CAST(stg_o.order_purchase_timestamp AS TIMESTAMP)))
        FROM staging.stg_orders stg_o
        JOIN staging.stg_customers stg_c ON stg_o.customer_id = stg_c.customer_id
        JOIN olist_dwh.dim_customers dim ON stg_c.customer_unique_id = dim.customer_unique_id
        WHERE stg_o.order_delivered_customer_date IS NOT NULL
          AND dim.is_current = TRUE;
        """

        with engine_dwh.connect() as conn:
            conn.execute(text(combined_transform_sql))
            conn.commit()
        
        print("✅ Transformasi SCD & Fakta Selesai!")

    # --- JIKA TERJADI ERROR, TAMPILKAN ALERT ---
    except Exception as e:
        print(f"\n⚠️ ALERT! Terjadi kesalahan pada proses ELT: {e}")
        # Raise e penting agar Luigi tahu kalau tugas ini gagal
        raise e 

if __name__ == "__main__":
    run_elt()