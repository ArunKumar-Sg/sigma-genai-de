
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



with __dbt__cte__stg_transactions as (
WITH cleaned_transactions AS (
    SELECT
        LOWER(transaction_id)   AS transaction_id,
        CAST(amount AS DECIMAL(10, 2)) AS amount,
        UPPER(status)           AS status,
        LOWER(merchant_id)      AS merchant_id,
        LOWER(customer_id)      AS customer_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        UPPER(payment_method)   AS payment_method,
        CURRENT_TIMESTAMP       AS loaded_at
    FROM SIGMA_DE.PUBLIC.fact_transactions
    WHERE NOT merchant_id LIKE 'TEST_%'
)

SELECT * FROM cleaned_transactions
),  __dbt__cte__mart_merchant_performance as (
WITH filtered_transactions AS (
    SELECT
        transaction_id,
        amount,
        status,
        merchant_id,
        customer_id,
        transaction_date,
        payment_method
    FROM __dbt__cte__stg_transactions
    WHERE status IN ('COMPLETED', 'FAILED')
),

merchant_details AS (
    SELECT
        merchant_id,
        merchant_name,
        category,
        city
    FROM SIGMA_DE.PUBLIC.dim_merchant
),

aggregated_metrics AS (
    SELECT
        ft.merchant_id,
        SUM(CASE WHEN ft.status = 'COMPLETED' THEN ft.amount ELSE 0 END)   AS total_revenue,
        COUNT(ft.transaction_id)                                             AS total_transactions,
        COUNT(CASE WHEN ft.status = 'FAILED' THEN 1 END)                    AS failed_count,
        ROUND(
            COUNT(CASE WHEN ft.status = 'FAILED' THEN 1 END) * 100.0
            / NULLIF(COUNT(ft.transaction_id), 0),
            2
        )                                                                     AS failure_rate_pct,
        AVG(CASE WHEN ft.status = 'COMPLETED' THEN ft.amount ELSE NULL END) AS avg_transaction_value,
        COUNT(DISTINCT ft.customer_id)                                       AS unique_customers
    FROM filtered_transactions ft
    GROUP BY ft.merchant_id
)

SELECT
    am.merchant_id,
    md.merchant_name,
    md.category,
    md.city,
    am.total_revenue,
    am.total_transactions,
    am.failed_count,
    am.failure_rate_pct,
    am.avg_transaction_value,
    am.unique_customers
FROM aggregated_metrics am
JOIN merchant_details md ON am.merchant_id = md.merchant_id
) select merchant_name
from __dbt__cte__mart_merchant_performance
where merchant_name is null



  
  
      
    ) dbt_internal_test