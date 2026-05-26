-- Bonus Challenge: test that failure_rate_pct is always between 0 and 100.
-- dbt singular test: returns rows that VIOLATE the condition.
-- If this query returns 0 rows → test PASSES.
-- If it returns any rows   → test FAILS (bad data detected).

SELECT
    merchant_id,
    failure_rate_pct
FROM {{ ref('mart_merchant_performance') }}
WHERE failure_rate_pct < 0
   OR failure_rate_pct > 100
