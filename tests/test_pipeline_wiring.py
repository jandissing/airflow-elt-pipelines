"""Tests that the XCom keys and task ids the tasks use actually line up.

These call the Airflow-facing task callables with a stand-in TaskInstance, so a
typo in an ``xcom_pull(task_ids=...)`` fails here instead of at 09:00 in prod.
"""

from io import StringIO

import pandas as pd
import pytest

from elt import extract_csv, transform_csv, transform_sales

HEADER = "customer_name,product_name,quantity,unit_price,sale_date,region\n"
ROWS = "Ana,Monitor,1,1200.00,2024-01-18,North\nBo,Mouse,2,150.00,2024-01-19,South\n"


class FakeTaskInstance:
    """Records pushes under the pushing task's id, like Airflow's XCom table."""

    def __init__(self):
        self.store = {}
        self.task_id = None

    def running_as(self, task_id):
        self.task_id = task_id
        return {"task_instance": self}

    def xcom_push(self, key, value):
        self.store[(self.task_id, key)] = value

    def xcom_pull(self, task_ids, key):
        return self.store.get((task_ids, key))


def test_csv_extract_hands_its_rows_to_the_aggregate_task(tmp_path, monkeypatch):
    (tmp_path / "sales.csv").write_text(HEADER + ROWS)
    monkeypatch.setattr(extract_csv, "INPUT_DIR", str(tmp_path))
    ti = FakeTaskInstance()

    row_count = extract_csv.extract_csv_files(**ti.running_as("extract_csv"))
    region_count = transform_csv.aggregate_by_region(
        **ti.running_as("aggregate_by_region")
    )

    assert row_count == 2
    assert region_count == 2


def test_aggregate_publishes_a_summary_the_load_task_can_read(tmp_path, monkeypatch):
    (tmp_path / "sales.csv").write_text(HEADER + ROWS)
    monkeypatch.setattr(extract_csv, "INPUT_DIR", str(tmp_path))
    ti = FakeTaskInstance()
    extract_csv.extract_csv_files(**ti.running_as("extract_csv"))
    transform_csv.aggregate_by_region(**ti.running_as("aggregate_by_region"))

    summary = pd.read_json(
        StringIO(ti.xcom_pull("aggregate_by_region", "region_summary"))
    )

    assert set(summary["region"]) == {"North", "South"}
    assert set(summary.columns) == {
        "region",
        "total_quantity",
        "total_revenue",
        "avg_unit_price",
        "num_orders",
    }


def test_sales_transform_fails_loudly_when_extract_pushed_nothing():
    ti = FakeTaskInstance()

    with pytest.raises(ValueError):
        transform_sales.transform_data(**ti.running_as("transform"))
