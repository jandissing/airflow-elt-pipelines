"""Extract sales data from CSV files in the input directory."""

import glob
import logging
import os

import pandas as pd

from elt.config import INPUT_DIR

logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    "customer_name",
    "product_name",
    "quantity",
    "unit_price",
    "sale_date",
    "region",
]


def read_csv_directory(input_dir: str) -> pd.DataFrame:
    """Read and concatenate every ``*.csv`` in ``input_dir``, validating the schema.

    Raises:
        FileNotFoundError: if the directory contains no CSV files.
        ValueError: if the combined frame is missing an expected column.
    """
    csv_files = sorted(glob.glob(os.path.join(input_dir, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {input_dir}. Add *.csv files and re-run."
        )

    logger.info(f"📄 Found {len(csv_files)} CSV file(s)")
    frames = []
    for path in csv_files:
        df_part = pd.read_csv(path)
        logger.info(f"   • {os.path.basename(path)}: {len(df_part)} rows")
        frames.append(df_part)

    df = pd.concat(frames, ignore_index=True)

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"CSV files are missing expected columns: {missing}. "
            f"Expected: {EXPECTED_COLUMNS}"
        )
    return df


def extract_csv_files(**context):
    """Airflow task: read the input CSVs and publish the rows to XCom.

    Returns:
        int: Total number of rows read across all files
    """
    try:
        logger.info("=" * 60)
        logger.info("🔍 EXTRACT (CSV) TASK STARTED")
        logger.info(f"📂 Input directory: {INPUT_DIR}")

        df = read_csv_directory(INPUT_DIR)
        logger.info(f"✓ Combined into {len(df)} total rows")
        logger.info(f"📋 Columns: {list(df.columns)}")
        logger.info("✓ Schema validation passed")

        raw_data_json = df.to_json(orient="records")
        context["task_instance"].xcom_push(key="csv_raw_data", value=raw_data_json)
        logger.info("✓ Data pushed to XCom")

        logger.info(f"✅ EXTRACT (CSV) TASK COMPLETED - {len(df)} rows")
        logger.info("=" * 60)
        return len(df)

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ EXTRACT (CSV) TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
