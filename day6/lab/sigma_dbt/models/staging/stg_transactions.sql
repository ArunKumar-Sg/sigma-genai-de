WITH cleaned_transactions AS (
    SELECT
        LOWER(transaction_id) AS transaction_id,
        CAST(amount AS DECIMAL(10, 2)) AS amount,
        LOWER(status) AS status,
        LOWER(merchant_id) AS merchant_id,
        LOWER(customer_id) AS customer_id,
        CAST(transaction_date AS DATE) AS transaction_date,
        LOWER(payment_method) AS payment_method,
        CURRENT_TIMESTAMP AS loaded_at
    FROM {{ source('sigma_analytics', 'fact_transactions') }}
    WHERE NOT merchant_id LIKE 'TEST_%'
)

SELECT * FROM cleaned_transactions
```

```yaml
version: 2

models:
  - name: stg_fact_transactions
    description: "Staged fact transactions with cleaned and transformed data"
    columns:
      - name: transaction_id
        description: "Unique identifier for each transaction"
        tests:
          - not_null
          - unique
      - name: amount
        description: "Transaction amount in USD"
        tests:
          - not_null
      - name: status
        description: "Status of the transaction (COMPLETED, FAILED, PENDING)"
        tests:
          - not_null
          - accepted_values:
              values: ['completed', 'failed', 'pending']
      - name: merchant_id
        description: "Foreign key referencing dim_merchant"
        tests:
          - not_null
          - relationships:
              to: ref('dim_merchant')
              field: merchant_id
      - name: customer_id
        description: "Foreign key referencing dim_customer"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
      - name: transaction_date
        description: "Date of the transaction"
        tests:
          - not_null
      - name: payment_method
        description: "Payment method used for the transaction"
        tests:
          - not_null
          - accepted_values:
              values: ['credit_card', 'debit_card', 'upi']
      - name: loaded_at
        description: "Timestamp when the data was loaded"
        tests:
          - not_null
