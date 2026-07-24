import json
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DB_CONNECTION = "postgresql+psycopg2://airflow_user:airflow_pass_2024@postgres:5432/airflow_db"
OUTPUT_DIR = "/opt/airflow/output"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "elt_pipeline_dag",
    default_args=default_args,
    schedule_interval="0 9 * * *",
    catchup=False,
    tags=["elt", "data-engineering"],
)


def extract_raw_data(**context):
    """Extract raw sales data from PostgreSQL."""
    try:
        logger.info("Starting data extraction from raw_sales_data table")

        engine = create_engine(DB_CONNECTION)
        query = "SELECT * FROM raw_sales_data ORDER BY sale_date"

        df = pd.read_sql(query, engine)
        engine.dispose()

        row_count = len(df)
        logger.info(f"Successfully extracted {row_count} rows from raw_sales_data")

        raw_data_json = df.to_json(orient="records")
        context["task_instance"].xcom_push(key="raw_data", value=raw_data_json)

        return row_count

    except Exception as e:
        logger.error(f"Error during data extraction: {str(e)}", exc_info=True)
        raise


def transform_data(**context):
    """Transform raw data by calculating total_amount."""
    try:
        logger.info("Starting data transformation")

        task_instance = context["task_instance"]
        raw_data_json = task_instance.xcom_pull(task_ids="extract", key="raw_data")

        if not raw_data_json:
            raise ValueError("No raw data found in XCom")

        df = pd.read_json(raw_data_json)

        df["total_amount"] = df["quantity"] * df["unit_price"]

        transformed_df = df[
            ["customer_name", "product_name", "total_amount", "sale_date", "region"]
        ].copy()

        row_count = len(transformed_df)
        logger.info(
            f"Transformation completed. {row_count} rows transformed with total_amount column calculated"
        )

        transformed_data_json = transformed_df.to_json(orient="records")
        task_instance.xcom_push(key="transformed_data", value=transformed_data_json)

        return row_count

    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}", exc_info=True)
        raise


def load_to_database(**context):
    """Load transformed data into transformed_sales table."""
    try:
        logger.info("Starting data load to database")

        task_instance = context["task_instance"]
        transformed_data_json = task_instance.xcom_pull(
            task_ids="transform", key="transformed_data"
        )

        if not transformed_data_json:
            raise ValueError("No transformed data found in XCom")

        df = pd.read_json(transformed_data_json)

        engine = create_engine(DB_CONNECTION)

        with engine.begin() as connection:
            connection.execute(text("DELETE FROM transformed_sales"))
            logger.info("Cleared existing data from transformed_sales table")

        df.to_sql("transformed_sales", engine, if_exists="append", index=False)

        row_count = len(df)
        logger.info(f"Successfully loaded {row_count} rows into transformed_sales table")

        engine.dispose()

        return row_count

    except Exception as e:
        logger.error(f"Error during data load: {str(e)}", exc_info=True)
        raise


def export_to_excel(**context):
    """Export transformed data to timestamped Excel file."""
    try:
        logger.info("Starting export to Excel")

        task_instance = context["task_instance"]
        transformed_data_json = task_instance.xcom_pull(
            task_ids="transform", key="transformed_data"
        )

        if not transformed_data_json:
            raise ValueError("No transformed data found in XCom")

        df = pd.read_json(transformed_data_json)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sales_report_{timestamp}.xlsx"
        filepath = os.path.join(OUTPUT_DIR, filename)

        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Sales Data", index=False)

            worksheet = writer.sheets["Sales Data"]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        logger.info(f"Excel file exported to {filepath}")

        total_records = len(df)
        total_revenue = df["total_amount"].sum()

        logger.info(f"Export Summary - Total Records: {total_records}, Total Revenue: ${total_revenue:.2f}")

        revenue_by_region = df.groupby("region")["total_amount"].sum()
        for region, revenue in revenue_by_region.items():
            logger.info(f"Revenue by Region - {region}: ${revenue:.2f}")

        return filepath

    except Exception as e:
        logger.error(f"Error during Excel export: {str(e)}", exc_info=True)
        raise


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
