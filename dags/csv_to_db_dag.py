import sys
from datetime import datetime, timedelta

# Ensure the project root is importable so local packages (csv_elt) resolve in
# task-worker subprocesses. Airflow 3.x runs DAGs from a versioned bundle copy,
# so a __file__-relative path points at the bundle temp dir, not the project
# root — hardcode the absolute mount path where the packages actually live.
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from csv_elt.extract import extract_csv_files
from csv_elt.transform import aggregate_by_region
from csv_elt.load import load_region_summary

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "csv_to_db_dag",
    default_args=default_args,
    schedule="0 8 * * *",
    catchup=False,
    tags=["elt", "csv", "data-engineering"],
)

extract_task = PythonOperator(
    task_id="extract_csv",
    python_callable=extract_csv_files,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="aggregate_by_region",
    python_callable=aggregate_by_region,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_summary",
    python_callable=load_region_summary,
    dag=dag,
)

extract_task >> transform_task >> load_task

__all__ = ["dag"]
