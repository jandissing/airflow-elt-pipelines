import sys
from datetime import datetime, timedelta

# Ensure the project root is importable so local packages (elt) resolve in
# task-worker subprocesses. Airflow 3.x runs DAGs from a versioned bundle copy,
# so a __file__-relative path points at the bundle temp dir, not the project
# root — hardcode the absolute mount path where the packages actually live.
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from elt.extract_sales import extract_raw_data
from elt.transform_sales import transform_data
from elt.load_sales import load_to_database
from elt.export_sales import export_to_excel

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "elt_pipeline_dag",
    default_args=default_args,
    schedule="0 9 * * *",
    catchup=False,
    tags=["elt", "data-engineering"],
)


extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract_raw_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load",
    python_callable=load_to_database,
    dag=dag,
)

export_task = PythonOperator(
    task_id="export",
    python_callable=export_to_excel,
    dag=dag,
)

extract_task >> transform_task >> [load_task, export_task]

__all__ = ["dag"]
