"""Data export operations"""

import logging
import os
from datetime import datetime
from io import StringIO

import pandas as pd

from elt.config import OUTPUT_DIR

logger = logging.getLogger(__name__)


def export_to_excel(**context):
    """
    Export transformed data to timestamped Excel file.

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

        logger.info(f"📂 Ensuring output directory exists: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        logger.info("✓ Output directory ready")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"sales_report_{timestamp}.xlsx"
        filepath = os.path.join(OUTPUT_DIR, filename)
        logger.info(f"📝 Output filename: {filename}")

        logger.info("🔧 Creating Excel workbook...")
        with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Sales Data", index=False)
            logger.info("✓ Data written to Excel")

            worksheet = writer.sheets["Sales Data"]
            logger.info("📐 Auto-adjusting column widths...")
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
            logger.info("✓ Column widths adjusted")

        logger.info(f"✅ Excel file created: {filepath}")

        total_records = len(df)
        total_revenue = df["total_amount"].sum()

        logger.info("=" * 60)
        logger.info("📈 EXPORT SUMMARY")
        logger.info(f"   Total Records: {total_records}")
        logger.info(f"   Total Revenue: ${total_revenue:,.2f}")

        revenue_by_region = df.groupby("region")["total_amount"].sum()
        logger.info("   Revenue by Region:")
        for region, revenue in revenue_by_region.items():
            logger.info(f"     • {region}: ${revenue:,.2f}")

        logger.info(f"✅ EXPORT TASK COMPLETED")
        logger.info("=" * 60)
        return filepath

    except Exception as e:
        logger.error("=" * 60)
        logger.error("❌ EXPORT TASK FAILED")
        logger.error(f"Error: {str(e)}", exc_info=True)
        logger.error("=" * 60)
        raise
