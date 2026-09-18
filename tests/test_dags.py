"""Import the real DAG files and assert their shape.

Skipped when Airflow is not installed locally; CI installs it and runs them.
"""

import pytest

pytest.importorskip("airflow", reason="Airflow not installed in this environment")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags")


def test_every_dag_file_imports_cleanly(dagbag):
    assert dagbag.import_errors == {}


def test_both_pipelines_are_registered(dagbag):
    assert {"elt_pipeline_dag", "csv_to_db_dag"} <= set(dagbag.dag_ids)


def test_sales_pipeline_runs_load_and_export_after_transform(dagbag):
    dag = dagbag.dags["elt_pipeline_dag"]

    assert dag.get_task("transform").upstream_task_ids == {"extract"}
    assert dag.get_task("load").upstream_task_ids == {"transform"}
    assert dag.get_task("export").upstream_task_ids == {"transform"}


def test_csv_pipeline_is_a_three_step_chain(dagbag):
    dag = dagbag.dags["csv_to_db_dag"]

    assert dag.get_task("aggregate_by_region").upstream_task_ids == {"extract_csv"}
    assert dag.get_task("load_summary").upstream_task_ids == {"aggregate_by_region"}


def test_tasks_retry_before_giving_up(dagbag):
    for dag_id in ("elt_pipeline_dag", "csv_to_db_dag"):
        for task in dagbag.dags[dag_id].tasks:
            assert task.retries >= 1
