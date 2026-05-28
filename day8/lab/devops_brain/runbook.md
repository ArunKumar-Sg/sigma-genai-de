# Pipeline Overview

This pipeline ingests raw transaction data, transforms it into a cleaned and enriched format, and computes merchant performance and daily summaries. It runs daily to ensure up-to-date analytics for business decision-making. If this pipeline fails, critical business metrics and reports will be outdated or incorrect.

## Pipeline Steps

1. Connect to DuckDB using `get_connection()`.
2. Set up required tables using `setup_tables()`.
3. Load merchant data using `load_merchants()`.
4. Load raw transactions into the bronze table using `load_bronze()`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver()`.
6. Load transformed transactions into the silver table using `load_silver()`.
7. Compute merchant performance metrics using `compute_merchant_performance()`.
8. Compute daily summary metrics using `compute_daily_summary()`.
9. Load computed metrics into the gold tables using `load_gold()`.

## Schedule / Trigger

This pipeline runs daily at 2 AM UTC via a cron job.

## Failure Modes

1. **DuckDB Connection Failure**
   - **Root Cause:** Database server is down.
   - **Symptom:** `get_connection()` fails.
2. **Table Setup Failure**
   - **Root Cause:** SQL syntax error.
   - **Symptom:** `setup_tables()` throws an exception.
3. **Merchant Data Load Failure**
   - **Root Cause:** Corrupt merchant data.
   - **Symptom:** `load_merchants()` fails to insert records.
4. **Bronze Table Load Failure**
   - **Root Cause:** Invalid transaction data.
   - **Symptom:** `load_bronze()` fails to insert records.
5. **Silver Table Transformation Failure**
   - **Root Cause:** Missing merchant IDs in transactions.
   - **Symptom:** `transform_bronze_to_silver()` produces incomplete records.

## Recovery Actions

1. **DuckDB Connection Failure**
   - Check DB server status.
   - Restart DB server if necessary.
   - Retry pipeline.
2. **Table Setup Failure**
   - Review SQL queries in `setup_tables()`.
   - Fix syntax errors.
   - Retry pipeline.
3. **Merchant Data Load Failure**
   - Validate merchant data integrity.
   - Correct corrupt records.
   - Retry pipeline.
4. **Bronze Table Load Failure**
   - Validate transaction data integrity.
   - Correct invalid records.
   - Retry pipeline.
5. **Silver Table Transformation Failure**
   - Ensure all transactions have valid merchant IDs.
   - Correct missing IDs.
   - Retry pipeline.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver()`.

## Escalation Contacts

1. **On-call DE:** Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead:** Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager:** Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the number of records in `silver_transactions` matches the input.
- Check `gold_merchant_performance` for expected merchant IDs.
- Ensure `gold_daily_summary` has today's date.