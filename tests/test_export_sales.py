"""Unit tests for the Excel export step."""

import re

import pandas as pd

from elt.export_sales import write_excel_report


def _transformed():
    return pd.DataFrame(
        [
            {
                "customer_name": "Ana",
                "product_name": "Monitor",
                "total_amount": 1200.0,
                "sale_date": "2024-01-18",
                "region": "North",
            }
        ]
    )


def test_writes_a_workbook_that_reads_back_with_the_same_rows(tmp_path):
    path = write_excel_report(_transformed(), str(tmp_path))

    written = pd.read_excel(path)
    assert len(written) == 1
    assert written.loc[0, "total_amount"] == 1200.0


def test_names_the_file_with_a_timestamp(tmp_path):
    path = write_excel_report(_transformed(), str(tmp_path))

    assert re.fullmatch(r"sales_report_\d{8}_\d{6}\.xlsx", path.rsplit("/", 1)[-1])


def test_creates_the_output_directory_when_it_is_missing(tmp_path):
    target = tmp_path / "does-not-exist-yet"

    path = write_excel_report(_transformed(), str(target))

    assert target.is_dir()
    assert path.startswith(str(target))
