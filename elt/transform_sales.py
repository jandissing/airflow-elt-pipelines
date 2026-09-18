"""Data transformation operations"""

import logging
from io import StringIO

import pandas as pd

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = ["customer_name", "product_name", "total_amount", "sale_date", "region"]


def transform_sales_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Add ``total_amount = quantity * unit_price`` and keep the columns we load.

    The input frame is not modified.

    Raises:
        KeyError: if a required source column is missing.
    """
    with_total = df.assign(total_amount=df["quantity"] * df["unit_price"])
    return with_total[OUTPUT_COLUMNS].copy()


def transform_data(**context):
    """Airflow task: read raw rows from XCom, publish the transformed rows.

    Returns:
        int: Number of rows transformed
    """
    try:
        logger.info("=" * 60)
        logger.info("🔄 TRANSFORM TASK STARTED")

        task_instance = context["task_instance"]
        logger.info("📥 Pulling raw_data from XCom (extract task)")
        raw_data_json = task_instance.xcom_pull(task_ids="extract", key="raw_data")

        if not raw_data_json:
            raise ValueError("No raw data found in XCom from extract task")
        logger.info("✓ Raw data retrieved")

        df = pd.read_json(StringIO(raw_data_json))
        logger.info(f"📊 Loaded {len(df)} rows from XCom")
        logger.info(f"📋 Input columns: {list(df.columns)}")

        logger.info("🧮 Calculating total_amount = quantity × unit_price")
        transformed_df = transform_sales_frame(df)
        logger.info(
            f"✓ Calculation complete. Min: ${transformed_df['total_amount'].min()}, "
            f"Max: ${transformed_df['total_amount'].max()}"
        )

        row_count = len(transformed_df)
        logger.info(f"✓ Selected {row_count} rows")
        logger.info(f"📋 Output columns: {list(transformed_df.columns)}")

        transformed_data_json = transformed_df.to_json(
            orient="records", date_format="iso"
        )
        task_instance.xcom_push(key="transformed_data", value=transformed_data_json)
        logger.info("✓ Transformed data pushed to XCom")

        logger.info(f"✅ TRANSFORM TASK COMPLETED - {row_count} rows")
        logger.info("=" * 60)
        return row_count

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ TRANSFORM TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
