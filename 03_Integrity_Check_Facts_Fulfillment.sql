-- Mengecek apakah ada data di tabel fakta yang tidak punya pasangan di dimensi (Orphan Records)
SELECT 
    COUNT(f.order_id) as total_transaksi,
    COUNT(d.customer_sk) as transaksi_valid,
    SUM(CASE WHEN d.customer_sk IS NULL THEN 1 ELSE 0 END) as data_error_null
FROM olist_dwh.fact_fulfillment f
LEFT JOIN olist_dwh.dim_customers d ON f.customer_sk = d.customer_sk;