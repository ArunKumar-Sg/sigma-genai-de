-- Revenue report by merchant
-- Bug 1 (Correctness): Missing STATUS = 'COMPLETED' filter — FAILED txns counted as revenue
-- Bug 2 (Performance): Implicit JOIN syntax instead of explicit JOIN
-- Bug 3 (Correctness): ORDER BY ascending — wrong for "top 10" use case

SELECT m.merchant_name,
       SUM(t.amount) as total_revenue,
       COUNT(*) as txn_count
FROM fact_transactions t, dim_merchant m
WHERE t.merchant_id = m.merchant_id
  AND t.transaction_date > '2024-01-01'
GROUP BY m.merchant_name
ORDER BY total_revenue
LIMIT 10;
