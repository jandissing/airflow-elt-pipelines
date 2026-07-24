import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def hello(**context):
    """Simple task that logs a greeting and returns a value."""
    logger.info("=" * 60)
    logger.info("👋 HELLO TASK STARTED")
    logger.info(f"   Run ID: {context.get('run_id')}")
    logger.info(f"   Logical date: {context.get('logical_date')}")
    logger.info("   Hello from the test DAG! Everything is working.")
    logger.info("✅ HELLO TASK COMPLETED")
    logger.info("=" * 60)
    return "success"


with DAG(
    "test_simple_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["test"],
) as dag:
    task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )
