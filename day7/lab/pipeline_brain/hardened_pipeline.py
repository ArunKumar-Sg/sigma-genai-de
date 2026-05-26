import shutil
import logging
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, broadcast, when, sum, count, max, min, expr, countDistinct
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
import os
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("[Stage: ingest_bronze] Starting ingestion")
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

        partition_path_transactions = f"{output_path}/transactions"
        shutil.rmtree(partition_path_transactions, ignore_errors=True)
        transactions_df.write.partitionBy("transaction_date").mode("overwrite").parquet(partition_path_transactions)

        partition_path_promotions = f"{output_path}/promotions"
        shutil.rmtree(partition_path_promotions, ignore_errors=True)
        promotions_df.write.partitionBy("promo_id").mode("overwrite").parquet(partition_path_promotions)

        logging.info(f"[Stage: ingest_bronze] Ingestion completed. Transactions: {transactions_df.count()}, Promotions: {promotions_df.count()}")
    except Exception as e:
        logging.error(f"[Stage: ingest_bronze] Error: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("[Stage: transform_silver] Starting transformation")
        transactions_bronze_df = (spark.read.format("parquet")
                                  .load(f"{bronze_path}/transactions")
                                  .filter(col("transaction_date") == run_date))

        promotions_bronze_df = (spark.read.format("parquet")
                                .load(f"{bronze_path}/promotions"))

        transactions_silver_df = (transactions_bronze_df.withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18,2)))
                                  .withColumn("transaction_date", col("transaction_date").cast(TimestampType()))
                                .where((col("payment_status") == "COMPLETED") &
                                         (col("transaction_amount") > 0) &
                                         (col("promo_id").isNotNull()) &
                                         (col("transaction_id").isNotNull())))

        promotions_silver_df = (promotions_bronze_df.withColumn("discount_pct", col("discount_pct").cast(DecimalType(5,2)))
                                .where((col("promo_id").isNotNull()) &
                                       (col("channel").isin("Email", "SMS", "Push")) &
                                       (col("discount_pct").between(0, 100))))

        transactions_silver_df = (transactions_silver_df.dropDuplicates(["transaction_id"])
                                                 .orderBy(col("ingestion_timestamp").desc())
                                                .withWatermark("ingestion_timestamp", "1 hour"))

        merchants_df = (spark.read.format("parquet")
                        .load(merchants_path)
                       .hint("broadcast"))

        transactions_silver_df = (transactions_silver_df.join(broadcast(merchants_df), "promo_id", "left")
                                  .withColumn("quality_flag", when(col("promo_id").isNotNull(), "CLEAN").otherwise("UNMATCHED")))

        partition_path_transactions = f"{output_path}/transactions"
        shutil.rmtree(partition_path_transactions, ignore_errors=True)
        transactions_silver_df.write.partitionBy("transaction_date").mode("overwrite").parquet(partition_path_transactions)

        partition_path_promotions = f"{output_path}/promotions"
        shutil.rmtree(partition_path_promotions, ignore_errors=True)
        promotions_silver_df.write.partitionBy("promo_id").mode("overwrite").parquet(partition_path_promotions)

        logging.info(f"[Stage: transform_silver] Transformation completed. Transactions: {transactions_silver_df.count()}, Promotions: {promotions_silver_df.count()}")
    except Exception as e:
        logging.error(f"[Stage: transform_silver] Error: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: build_merchant_performance] Starting aggregation")
        transactions_df = (spark.read.parquet(silver_path)
                           .filter(col("transaction_date") == run_date)
                           .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18,2)))
                           .withColumn("transaction_date", col("transaction_date").cast(TimestampType())))

        completed_transactions_df = transactions_df.filter(col("payment_status") == "COMPLETED")

        merchant_performance_df = (completed_transactions_df.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date")
                                   .agg(sum("transaction_amount").alias("total_revenue"),
                                        count("*").alias("txn_count"),
                                        (count(when(col("payment_status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")))

        partition_path = output_path
        shutil.rmtree(partition_path, ignore_errors=True)
        merchant_performance_df.write.partitionBy("transaction_date").mode("overwrite").parquet(partition_path)

        logging.info(f"[Stage: build_merchant_performance] Aggregation completed. Rows: {merchant_performance_df.count()}")
    except Exception as e:
        logging.error(f"[Stage: build_merchant_performance] Error: {e}")
        raise

def build_customer_ltv(spark, silver_path):
    try:
        logging.info("[Stage: build_customer_ltv] Starting aggregation")
        transactions_df = (spark.read.parquet(silver_path)
                          .filter(col("payment_status") == "COMPLETED")
                           .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18,2))))

        customer_ltv_df = (transactions_df.groupBy("customer_id") 
                           .agg(sum("transaction_amount").alias("total_spent"),
                                count("*").alias("total_txns"),
                                avg("transaction_amount").alias("avg_txn_value"),
                                min("transaction_date").alias("first_txn_date"),
                                max("transaction_date").alias("last_txn_date"),
                                broadcast(transactions_df.groupBy("customer_id", "payment_method")
                                                        .count())
                                                        .alias("payment_methods")
                                                         .selectExpr("customer_id", 
                                                                      expr("mode(payment_method) within group (order by count) as preferred_payment_method"))))

        output_path = silver_path.replace("silver", "ltv")
        shutil.rmtree(output_path, ignore_errors=True)
        customer_ltv_df.write.mode("overwrite").parquet(output_path)

        logging.info(f"[Stage: build_customer_ltv] Aggregation completed. Rows: {customer_ltv_df.count()}")
    except Exception as e:
        logging.error(f"[Stage: build_customer_ltv] Error: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("[Stage: build_daily_summary] Starting aggregation")
        transactions_df = (spark.read.parquet(silver_path)
                         .filter(col("transaction_date") == run_date)
                         .withColumn("transaction_amount", col("transaction_amount").cast(DecimalType(18,2)))
                          .filter(col("payment_status") == "COMPLETED"))

        daily_summary_df = (transactions_df.groupBy("transaction_date")
                          .agg(sum("transaction_amount").alias("total_revenue"),
                                 count("*").alias("total_txns"),
                                 countDistinct("customer_id").alias("unique_customers"),
                                 countDistinct("merchant_id").alias("unique_merchants"),
                                 (count(when(col("payment_status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")))

        partition_path = output_path
        shutil.rmtree(partition_path, ignore_errors=True)
        daily_summary_df.write.partitionBy("transaction_date").mode("overwrite").parquet(partition_path)

        logging.info(f"[Stage: build_daily_summary] Aggregation completed. Rows: {daily_summary_df.count()}")
    except Exception as e:
        logging.error(f"[Stage: build_daily_summary] Error: {e}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("[Stage: run_gold] Starting gold layer processing")
        merchant_performance_output = f"{gold_output_dir}/merchant_performance"
        customer_ltv_output = f"{gold_output_dir}/customer_ltv"
        daily_summary_output = f"{gold_output_dir}/daily_summary"

        build_merchant_performance(spark, silver_path, merchant_performance_output, run_date)
        build_customer_ltv(spark, silver_path)
        build_daily_summary(spark, silver_path, daily_summary_output, run_date)

        run_metadata = {
            "pipeline_name": "Sigma DataTech Transaction Analytics Pipeline",
            "run_date": run_date,
            "run_id": "run_001",
            "run_status": "SUCCESS",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat(),
            "output_paths": {
                "merchant_performance": merchant_performance_output,
                "customer_ltv": customer_ltv_output,
                "daily_summary": daily_summary_output
            }
        }

        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f, indent=4)

        logging.info("[Stage: run_gold] Gold layer processing completed successfully")
    except Exception as e:
        logging.error(f"[Stage: run_gold] Error: {e}")
        run_metadata["run_status"] = "FAILED"
        run_metadata["error_message"] = str(e)
        with open(f"{gold_output_dir}/run_metadata_{run_date}.json", "w") as f:
            json.dump(run_metadata, f, indent=4)
        raise

if __name__ == "__main__":
    spark = (SparkSession.builder
           .appName("Marketing Attribution Pipeline")
            .getOrCreate())

    input_path = os.environ["INPUT_PATH"]
    bronze_path = os.environ["BRONZE_PATH"]
    silver_path = os.environ["SILVER_PATH"]
    merchants_path = os.environ["MERCHANTS_PATH"]
    gold_output_dir = os.environ["GOLD_OUTPUT_DIR"]
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    run_id = "run_001"

    ingest_bronze(spark, input_path, bronze_path, run_date, run_id)
    transform_silver(spark, bronze_path, merchants_path, silver_path, run_date)
    run_gold(spark, silver_path, gold_output_dir, run_date)
