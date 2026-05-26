"""
Sigma DataTech Transaction Analytics Pipeline
Fixed version — addresses code review findings from 5_code_review.py

FIX 1 (FAIL): ERROR_HANDLING  — Added try/except blocks around every pipeline stage.
FIX 2 (WARN): ROW_COUNT_LOGGING — Added logging.info(.count()) after key transformations.

Architecture: Bronze -> Silver -> Gold (medallion pattern)
"""

# ═══════════════════════════════════════════════════════════════
# SECTION 1: BRONZE + SILVER LAYERS
# ═══════════════════════════════════════════════════════════════
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, broadcast, when
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

import warnings
warnings.filterwarnings("ignore")

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    """Ingest raw transactions and promotions into the Bronze layer."""
    try:
        logging.info("[ingest_bronze] Starting ingestion")

        transactions_df = (spark.read.format("parquet")
                          .option("header", "true")
                           .load(f"{input_path}/transactions")
                           .withColumn("ingestion_timestamp", current_timestamp())
                           .withColumn("source_file_name", lit("transactions.parquet"))
                           .withColumn("batch_id", lit(run_id)))

        promotions_df = (spark.read.format("csv")
                        .option("header", "true")
                        .load(f"{input_path}/promotions")
                        .withColumn("ingestion_timestamp", current_timestamp())
                        .withColumn("source_file_name", lit("promotions.csv"))
                        .withColumn("batch_id", lit(run_id)))

        # Write data to Bronze layer
        transactions_df.write.partitionBy("transaction_date").parquet(f"{output_path}/transactions")
        promotions_df.write.partitionBy("promo_id").parquet(f"{output_path}/promotions")

        # FIX 2: Row count logging after write
        txn_count = transactions_df.count()
        promo_count = promotions_df.count()
        logging.info(f"[ingest_bronze] Complete — transactions: {txn_count}, promotions: {promo_count}")

    except Exception as e:
        logging.error(f"[ingest_bronze] FAILED: {e}")
        raise


def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    """Apply quality filters, deduplication, and merchant enrichment into the Silver layer."""
    try:
        logging.info("[transform_silver] Starting transformation")

        transactions_bronze_df = (spark.read.format("parquet")
                                  .load(f"{bronze_path}/transactions")
                                  .where(col("transaction_date") == run_date))

        promotions_bronze_df = (spark.read.format("parquet")
                                .load(f"{bronze_path}/promotions"))

        # Cast columns and apply business filters
        transactions_silver_df = (transactions_bronze_df
                                  .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18, 2)))
                                  .withColumn("transaction_date", col("transaction_date").cast(TimestampType()))
                                  .where((col("payment_status") == "COMPLETED") &
                                         (col("transaction_amount") > 0) &
                                         (col("promo_id").isNotNull()) &
                                         (col("transaction_id").isNotNull())))

        promotions_silver_df = (promotions_bronze_df
                                .withColumn("discount_pct", col("discount_pct").cast(DecimalType(5, 2)))
                                .where((col("promo_id").isNotNull()) &
                                       (col("channel").isin("Email", "SMS", "Push")) &
                                       (col("discount_pct").between(0, 100))))

        # Deduplicate and join with merchants dimension
        transactions_silver_df = (transactions_silver_df
                                  .dropDuplicates(["transaction_id"])
                                  .orderBy(col("ingestion_timestamp").desc())
                                  .withWatermark("ingestion_timestamp", "1 hour"))

        merchants_df = (spark.read.format("parquet")
                        .load(merchants_path)
                        .hint("broadcast"))

        transactions_silver_df = (transactions_silver_df
                                  .join(broadcast(merchants_df), "promo_id", "left")
                                  .withColumn("quality_flag",
                                              when(col("promo_id").isNotNull(), "CLEAN").otherwise("UNMATCHED")))

        # Write data to Silver layer
        transactions_silver_df.write.partitionBy("transaction_date").parquet(f"{output_path}/transactions")
        promotions_silver_df.write.partitionBy("promo_id").parquet(f"{output_path}/promotions")

        # FIX 2: Row count logging after transformation
        txn_count = transactions_silver_df.count()
        promo_count = promotions_silver_df.count()
        logging.info(f"[transform_silver] Complete — silver transactions: {txn_count}, promotions: {promo_count}")

    except Exception as e:
        logging.error(f"[transform_silver] FAILED: {e}")
        raise


def main():
    """Entry point — orchestrates Bronze and Silver stages."""
    spark = (SparkSession.builder
             .appName("Marketing Attribution Pipeline — Fixed")
             .getOrCreate())

    # Paths come from environment variables (no hardcoded strings)
    input_path = os.environ["INPUT_PATH"]
    bronze_path = os.environ["BRONZE_PATH"]
    silver_path = os.environ["SILVER_PATH"]
    merchants_path = os.environ["MERCHANTS_PATH"]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = f'run_{datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")}'

    ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
    transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════
# SECTION 2: GOLD AGGREGATION LAYER
# ═══════════════════════════════════════════════════════════════
import json
from pyspark.sql.functions import col, sum, count, max, min, expr, broadcast, when, countDistinct, avg
from pyspark.sql.types import DecimalType, TimestampType


def build_merchant_performance(spark, silver_path, output_path, run_date):
    """Aggregate per-merchant revenue and failure rate into the Gold layer."""
    try:
        logging.info("[build_merchant_performance] Starting aggregation")

        transactions_df = (spark.read.parquet(silver_path)
                           .filter(col("transaction_date") == run_date)
                           .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18, 2)))
                           .withColumn("transaction_date", col("transaction_date").cast(TimestampType())))

        completed_transactions_df = transactions_df.filter(col("payment_status") == "COMPLETED")

        merchant_performance_df = (completed_transactions_df
                                   .groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date")
                                   .agg(sum("transaction_amount").alias("total_revenue"),
                                        count("*").alias("txn_count"),
                                        (count(when(col("payment_status") == "FAILED", 1)) / count("*") * 100)
                                        .alias("failure_rate_pct")))

        merchant_performance_df.write.partitionBy("transaction_date").parquet(output_path)

        # FIX 2: Row count logging
        logging.info(f"[build_merchant_performance] Complete — rows: {merchant_performance_df.count()}")

    except Exception as e:
        logging.error(f"[build_merchant_performance] FAILED: {e}")
        raise


def build_customer_ltv(spark, silver_path):
    """Aggregate lifetime-value metrics per customer into the Gold layer."""
    try:
        logging.info("[build_customer_ltv] Starting aggregation")

        transactions_df = (spark.read.parquet(silver_path)
                            .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18, 2)))
                           .filter(col("payment_status") == "COMPLETED"))

        customer_ltv_df = (transactions_df.groupBy("customer_id")
                           .agg(sum("transaction_amount").alias("total_spent"),
                                count("*").alias("total_txns"),
                                avg("transaction_amount").alias("avg_txn_value"),
                                min("transaction_date").alias("first_txn_date"),
                                max("transaction_date").alias("last_txn_date")))

        output_path = silver_path.replace("silver", "ltv")
        customer_ltv_df.write.parquet(output_path)

        # FIX 2: Row count logging
        logging.info(f"[build_customer_ltv] Complete — customer rows: {customer_ltv_df.count()}")

    except Exception as e:
        logging.error(f"[build_customer_ltv] FAILED: {e}")
        raise


def build_daily_summary(spark, silver_path, output_path, run_date):
    """Aggregate a single-row daily summary for the given run_date."""
    try:
        logging.info("[build_daily_summary] Starting aggregation")

        transactions_df = (spark.read.parquet(silver_path)
                          .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18, 2)))
                          .filter(col("payment_status") == "COMPLETED"))

        daily_summary_df = (transactions_df.groupBy("transaction_date")
                           .agg(sum("transaction_amount").alias("total_revenue"),
                                 count("*").alias("total_txns"),
                                 countDistinct("customer_id").alias("unique_customers"),
                                 countDistinct("merchant_id").alias("unique_merchants"),
                                 (count(when(col("payment_status") == "FAILED", 1)) / count("*") * 100)
                                 .alias("failure_rate_pct")))

        daily_summary_df.write.partitionBy("transaction_date").parquet(output_path)

        # FIX 2: Row count logging
        logging.info(f"[build_daily_summary] Complete — summary rows: {daily_summary_df.count()}")

    except Exception as e:
        logging.error(f"[build_daily_summary] FAILED: {e}")
        raise


def run_gold(spark, silver_path, gold_output_dir, run_date):
    """Orchestrate all Gold-layer aggregations and write run metadata."""
    try:
        logging.info("[run_gold] Starting gold layer processing")

        merchant_performance_output = f"{gold_output_dir}/merchant_performance"
        customer_ltv_output = f"{gold_output_dir}/customer_ltv"
        daily_summary_output = f"{gold_output_dir}/daily_summary"

        build_merchant_performance(spark, silver_path, merchant_performance_output, run_date)
        build_customer_ltv(spark, silver_path)
        build_daily_summary(spark, silver_path, daily_summary_output, run_date)

        run_metadata = {
            "run_date": run_date,
            "output_paths": {
                "merchant_performance": merchant_performance_output,
                "customer_ltv": customer_ltv_output,
                "daily_summary": daily_summary_output,
            }
        }

        spark.sparkContext.parallelize([run_metadata]).write.json(f"{gold_output_dir}/run_metadata")
        logging.info("[run_gold] Gold layer processing completed successfully")

    except Exception as e:
        logging.error(f"[run_gold] FAILED: {e}")
        raise


if __name__ == "__main__":
    spark = (SparkSession.builder
            .appName("Marketing Attribution Pipeline — Fixed")
             .getOrCreate())

    silver_path = os.environ["SILVER_PATH"]
    gold_output_dir = os.environ["GOLD_OUTPUT_DIR"]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    run_gold(spark, silver_path, gold_output_dir, run_date)
