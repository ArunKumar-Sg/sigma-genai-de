-- Daily failure rate by transaction date
-- Bug 1 (Performance): Correlated subquery re-scans full table per date group
-- Bug 2 (Correctness): Integer division — failed_count/total_count gives 0 for rates < 100%
-- Bug 3 (Correctness): References alias in same SELECT clause — will error in most engines

SELECT transaction_date,
       (SELECT COUNT(*)
        FROM fact_transactions f2
        WHERE f2.status = 'FAILED'
          AND f2.transaction_date = t.transaction_date) AS failed_count,
       COUNT(*) AS total_count,
       failed_count / total_count * 100 AS failure_rate
FROM fact_transactions t
GROUP BY transaction_date
ORDER BY transaction_date;
