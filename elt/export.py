"""Data export operations"""

import logging
import os
from datetime import datetime

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

        logger.info(
            f"Export Summary - Total Records: {total_records}, Total Revenue: ${total_revenue:.2f}"
        )

        revenue_by_region = df.groupby("region")["total_amount"].sum()
        for region, revenue in revenue_by_region.items():
            logger.info(f"Revenue by Region - {region}: ${revenue:.2f}")

        return filepath

    except Exception as e:
        logger.error(f"Error during Excel export: {str(e)}", exc_info=True)
        raise
