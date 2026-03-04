import pandas as pd
from sqlalchemy import create_engine, text
import os

# 1. Konfigurasi Koneksi Database
SRC_DB_URL = 'postgresql://postgres:postgres@localhost:5433/olist_src'
engine_src = create_engine(SRC_DB_URL)

# 2. Lokasi folder (Sudah benar di ./)
csv_folder_path = './' 

# 3. Dictionary pemetaan
files_to_load = {
    'olist_customers_dataset.csv': 'customers',
    'olist_geolocation_dataset.csv': 'geolocation',
    'olist_order_items_dataset.csv': 'order_items',
    'olist_order_payments_dataset.csv': 'order_payments',
    'olist_order_reviews_dataset.csv':'order_reviews',
    'olist_orders_dataset.csv': 'orders',
    'olist_products_dataset.csv': 'products',
    'olist_sellers_dataset.csv': 'sellers',
    'product_category_name_translation.csv': 'product_category'
}

def load_csv_to_src():
    print("Mulai proses Load data ke olist-src...")
    
    # --- FITUR BARU: Otomatis membuat schema jika belum ada ---
    with engine_src.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS olist_src;"))
        conn.commit()
        print("✅ Schema 'olist_src' sudah siap!")
    # ----------------------------------------------------------
    
    for file_name, table_name in files_to_load.items():
        file_path = os.path.join(csv_folder_path, file_name)
        
        if os.path.exists(file_path):
            print(f"Membaca {file_name}...")
            df = pd.read_csv(file_path, dtype=str)
            
            print(f"Memasukkan data ke tabel {table_name}...")
            df.to_sql(table_name, engine_src, schema='olist_src', if_exists='replace', index=False)
            print(f"✅ Sukses load {len(df)} baris ke tabel {table_name}\n")
        else:
            print(f"❌ File {file_name} tidak ditemukan di {file_path}\n")
            
    print("🎉 Semua proses Load selesai!")

if __name__ == "__main__":
    load_csv_to_src()