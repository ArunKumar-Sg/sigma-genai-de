-- Customer lifetime value report
-- Bug 1 (Security): Exposes customer email (PII) in output
-- Bug 2 (Correctness): LEFT JOIN + WHERE on right table silently converts to INNER JOIN
-- Bug 3 (Correctness): GROUP BY missing c.tier — will error in strict SQL engines
-- Bug 4 (Performance): No date filter — full table scan

SELECT c.customer_name, c.email, c.tier,
       SUM(t.amount) as lifetime_value,
       COUNT(t.transaction_id) as total_orders
FROM dim_customer c
LEFT JOIN fact_transactions t ON c.customer_id = t.customer_id
WHERE t.status = 'COMPLETED'
GROUP BY c.customer_name, c.email
HAVING SUM(t.amount) > 1000
ORDER BY lifetime_value DESC;
