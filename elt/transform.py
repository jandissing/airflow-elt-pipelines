"""Data transformation operations"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_data(**context):
    """
    Transform raw data by calculating total_amount column.

    Returns:
        int: Number of rows transformed
    """
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
