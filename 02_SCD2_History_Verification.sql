-- Menampilkan riwayat perpindahan kota (Hanya yang benar-benar berubah secara fisik)
SELECT 
    customer_unique_id, 
    customer_city, 
    is_current, 
    effective_date
FROM olist_dwh.dim_customers
WHERE customer_unique_id IN (
    SELECT customer_unique_id 
    FROM olist_dwh.dim_customers 
    GROUP BY customer_unique_id 
    HAVING COUNT(DISTINCT customer_city) > 1
)
ORDER BY customer_unique_id, effective_date ASC;