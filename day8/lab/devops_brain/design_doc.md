# Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data, enriches it with merchant details, and processes it into clean, enriched, and aggregated layers. It transforms raw transaction data into a usable format and computes merchant performance and daily summaries.

## Data Flow Diagram

```
+---------------------+     +----------------------+     +--------------------+     +--------------------+     +--------------------+
| Source: Transactions| --> | Bronze: bronze_txns  | --> | Silver: silver_txns| --> | Gold: merchant_perf | --> | Gold: daily_summary |
|                     |     |                      |     |                    |     |                    |     |                    |
+---------------------+     +----------------------+     +--------------------+     +--------------------+     +--------------------+
                 | Transforms:        | Transforms:         | Aggregates:           | Aggregates:           |
                 | - Filter negative  | - Enrich with       | - Compute merchant    | - Compute daily       |
                 | - Remove duplicates | merchant details    | performance           | summaries             |
                 |                    |                     |                       |                       |
```

## Key Design Decisions
- **Layered Processing**: Separates raw data ingestion, transformation, and aggregation into distinct layers (Bronze, Silver, Gold) for clarity and maintainability.
- **Data Enrichment**: Enriches transaction data with merchant details to provide more context and value.
- **Quality Flags**: Adds quality flags to transactions to identify and handle dirty data.
- **Aggregations**: Computes both merchant-specific and daily aggregated metrics to support different types of analysis.

## Known Limitations
- **Data Volume**: The pipeline is not optimized for very large datasets and may need adjustments for high-volume data.
- **Error Handling**: Basic error handling is implemented; more robust error logging and recovery mechanisms could be added.
- **Data Freshness**: The pipeline runs once per execution; it does not support incremental loads or real-time processing.
- **Schema Changes**: The pipeline does not handle schema changes in the source data; manual intervention is required for schema updates.

## Dependencies
- **DuckDB**: The database engine used for storing and querying data.
- **MERCHANTS**: A predefined list of merchant details required for data enrichment.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY**: Source files containing clean and dirty transaction data, respectively.