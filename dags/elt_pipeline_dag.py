from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from elt.extract import extract_raw_data
from elt.transform import transform_data
from elt.load import load_to_database
from elt.export import export_to_excel

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
