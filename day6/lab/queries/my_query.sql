-- Daily failure rate analysis per payment method
-- Written by: Student (Stretch Goal — intentional bugs planted below)
--
-- INTENTIONAL BUG #1 (Performance): Correlated subquery in SELECT
--   The subquery re-scans fact_transactions for every row in the outer query.
--   At scale this is O(n²) — should be replaced with a window function or CTE.
--
-- INTENTIONAL BUG #2 (Correctness): Integer division truncation
--   failed_count / total_count produces 0 for any rate < 100% in integer arithmetic.
--   Must cast to DECIMAL/FLOAT: failed_count * 1.0 / total_count * 100.
--
-- INTENTIONAL BUG #3 (Security): No WHERE clause on sensitive customer data
--   Joining dim_customer and exposing email without any filter — all PII visible.
--
-- INTENTIONAL BUG #4 (Readability): No aliases on subquery columns, cryptic col names
--   Column "x" and "y" have no meaning; reviewers cannot understand intent.

SELECT t.transaction_date,
       t.payment_method,
       c.email,                          -- BUG 3: PII exposed, no business need
       COUNT(*) AS total_count,
       (SELECT COUNT(*)                  -- BUG 1: correlated subquery, runs per group
        FROM fact_transactions f2
        WHERE f2.status = 'FAILED'
          AND f2.payment_method = t.payment_method
          AND f2.transaction_date = t.transaction_date
       ) AS x,                           -- BUG 4: cryptic column name
       x / total_count * 100 AS y        -- BUG 2: integer division, alias ref in same SELECT
FROM fact_transactions t,               -- BUG bonus: implicit join
     dim_customer c
GROUP BY t.transaction_date, t.payment_method, c.email
ORDER BY t.transaction_date;
