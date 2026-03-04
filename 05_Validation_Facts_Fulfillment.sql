-- Analisis Bisnis: Memastikan durasi pengiriman terhitung dan terhubung ke profil pelanggan
SELECT 
    f.order_id, 
    d.customer_unique_id, 
    d.customer_city, 
    f.delivery_actual_days, 
    d.is_current
FROM olist_dwh.fact_fulfillment f
JOIN olist_dwh.dim_customers d ON f.customer_sk = d.customer_sk
ORDER BY d.customer_unique_id, f.order_purchase_date DESC
LIMIT 12;