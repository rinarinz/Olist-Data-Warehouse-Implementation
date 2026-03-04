-- Validasi teknis mekanisme Expire & Insert (Mengecek status True/False dan Tanggal)
SELECT 
    customer_unique_id, 
    customer_city, 
    effective_date, 
    expiry_date, 
    is_current
FROM olist_dwh.dim_customers
WHERE customer_unique_id IN (
    SELECT customer_unique_id 
    FROM olist_dwh.dim_customers 
    GROUP BY customer_unique_id 
    HAVING COUNT(*) > 1
)
ORDER BY customer_unique_id, effective_date ASC
LIMIT 12;