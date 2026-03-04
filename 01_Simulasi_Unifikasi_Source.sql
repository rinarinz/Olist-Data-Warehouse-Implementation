-- Mengatasi data ganda di sumber agar menjadi satu alamat tunggal (Unifikasi)
UPDATE olist_src.customers AS c
SET customer_city = sub.old_city
FROM (
    SELECT customer_unique_id, MIN(customer_city) as old_city
    FROM olist_src.customers
    GROUP BY customer_unique_id
    HAVING COUNT(DISTINCT customer_city) > 1
) AS sub
WHERE c.customer_unique_id = sub.customer_unique_id;

-- Verifikasi: Hasil harus 0
SELECT COUNT(*) FROM (
    SELECT customer_unique_id FROM olist_src.customers 
    GROUP BY 1 HAVING COUNT(DISTINCT customer_city) > 1
) AS check_unification;