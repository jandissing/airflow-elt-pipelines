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
        logger.info("=" * 60)
        logger.info("🔄 TRANSFORM TASK STARTED")

        task_instance = context["task_instance"]
        logger.info("📥 Pulling raw_data from XCom (extract task)")
        raw_data_json = task_instance.xcom_pull(task_ids="extract", key="raw_data")

        if not raw_data_json:
            raise ValueError("No raw data found in XCom from extract task")
        logger.info("✓ Raw data retrieved")

        df = pd.read_json(raw_data_json)
        logger.info(f"📊 Loaded {len(df)} rows from XCom")
        logger.info(f"📋 Input columns: {list(df.columns)}")

        logger.info("🧮 Calculating total_amount = quantity × unit_price")
        df["total_amount"] = df["quantity"] * df["unit_price"]
        logger.info(f"✓ Calculation complete. Min: ${df['total_amount'].min()}, Max: ${df['total_amount'].max()}")

        logger.info("✂️ Selecting final columns")
        transformed_df = df[
            ["customer_name", "product_name", "total_amount", "sale_date", "region"]
        ].copy()

        row_count = len(transformed_df)
        logger.info(f"✓ Selected {row_count} rows")
        logger.info(f"📋 Output columns: {list(transformed_df.columns)}")

        transformed_data_json = transformed_df.to_json(orient="records")
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
