from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging
import json

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

# Initialize the DAG
dag = DAG(
    dag_id='sigma_transaction_pipeline',
    default_args=default_args,
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions",
    sla_miss_callback=lambda context: logging.warning(f"SLA miss for dag_id: {context['dag'].dag_id}, execution_date: {context['execution_date']}"),
)

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    error_message = context['exception']
    logging.error(f"Task failed. dag_id: {dag_id}, task_id: {task_id}, execution_date: {execution_date}, error: {error_message}")

def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet"""
    logging.info("Starting extract_bronze task")
    # Placeholder for actual extraction logic
    logging.info("Finished extract_bronze task")
    raise Exception("Placeholder failure")

def transform_silver(**context):
    """Clean, enrich, deduplicate to Silver"""
    logging.info("Starting transform_silver task")
    # Placeholder for actual transformation logic
    logging.info("Finished transform_silver task")
    raise Exception("Placeholder failure")

def build_gold(**context):
    """Generate the 3 Gold aggregation tables"""
    logging.info("Starting build_gold task")
    # Placeholder for actual build logic
    logging.info("Finished build_gold task")
    raise Exception("Placeholder failure")

# Define tasks with on_failure_callback
extract_bronze_task = PythonOperator(
    task_id='extract_bronze',
    python_callable=extract_bronze,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id='transform_silver',
    python_callable=transform_silver,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

build_gold_task = PythonOperator(
    task_id='build_gold',
    python_callable=build_gold,
    on_failure_callback=on_failure_callback,
    dag=dag,
)

# Task dependencies
extract_bronze_task >> transform_silver_task >> build_gold_task
