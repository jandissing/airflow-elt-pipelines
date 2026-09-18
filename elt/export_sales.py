"""Data export operations"""

import logging
import os
from datetime import datetime
from io import StringIO

import pandas as pd

from elt.config import OUTPUT_DIR

logger = logging.getLogger(__name__)

MAX_COLUMN_WIDTH = 50


def write_excel_report(df: pd.DataFrame, output_dir: str) -> str:
    """Write ``df`` to a timestamped ``.xlsx`` in ``output_dir``.

    Creates the directory if needed and widens each column to fit its contents.

    Returns:
        str: Path to the written file
    """
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(output_dir, f"sales_report_{timestamp}.xlsx")

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Sales Data", index=False)

        worksheet = writer.sheets["Sales Data"]
        for column in worksheet.columns:
            widest = max(len(str(cell.value)) for cell in column)
            column_letter = column[0].column_letter
            worksheet.column_dimensions[column_letter].width = min(
                widest + 2, MAX_COLUMN_WIDTH
            )

    return filepath


def export_to_excel(**context):
    """Airflow task: export the transformed rows to a timestamped Excel file.

    Returns:
        str: Path to exported file
    """
    try:
        logger.info("=" * 60)
        logger.info("📊 EXPORT TASK STARTED")

        task_instance = context["task_instance"]
        logger.info("📥 Pulling transformed_data from XCom (transform task)")
        transformed_data_json = task_instance.xcom_pull(
            task_ids="transform", key="transformed_data"
        )

        if not transformed_data_json:
            raise ValueError("No transformed data found in XCom from transform task")
        logger.info("✓ Transformed data retrieved")

        df = pd.read_json(StringIO(transformed_data_json))
        logger.info(f"📊 Loaded {len(df)} rows for export")

        logger.info(f"🔧 Writing workbook to {OUTPUT_DIR}...")
        filepath = write_excel_report(df, OUTPUT_DIR)
        logger.info(f"✅ Excel file created: {filepath}")

        logger.info("=" * 60)
        logger.info("📈 EXPORT SUMMARY")
        logger.info(f"   Total Records: {len(df)}")
        logger.info(f"   Total Revenue: ${df['total_amount'].sum():,.2f}")

        revenue_by_region = df.groupby("region")["total_amount"].sum()
        logger.info("   Revenue by Region:")
        for region, revenue in revenue_by_region.items():
            logger.info(f"     • {region}: ${revenue:,.2f}")

        logger.info("✅ EXPORT TASK COMPLETED")
        logger.info("=" * 60)
        return filepath

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ EXPORT TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
