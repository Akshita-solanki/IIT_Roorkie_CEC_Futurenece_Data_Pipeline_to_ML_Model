"""Airflow metadata and dependency checks for the NammaMart DAGs."""

from airflow.models import DagBag
from pathlib import Path


def test_nammamart_dags_parse_and_use_ist_schedule():
    bag = DagBag(include_examples=False)
    relevant_errors = {
        path: message
        for path, message in bag.import_errors.items()
        if "nammamart_" in path
    }
    assert not relevant_errors

    for dag_id in ["nammamart_etl_akshita", "nammamart_elt_akshita"]:
        dag = bag.get_dag(dag_id)
        assert dag is not None
        assert dag.catchup is False
        assert "Asia/Kolkata" in str(dag.timezone)
        assert dag.default_args["retries"] == 2
        assert dag.default_args["retry_delay"].total_seconds() == 120
        assert dag.default_args["execution_timeout"].total_seconds() == 1800
        assert dag.max_active_runs == 1
        schedule = getattr(dag, "schedule_interval", None) or getattr(dag, "schedule", None)
        expression = schedule if isinstance(schedule, str) else getattr(schedule, "_expression", "")
        assert expression == "0 9 * * 1-6"


def test_etl_extracts_five_sources_in_parallel_before_validation():
    dag = DagBag(include_examples=False).get_dag("nammamart_etl_akshita")
    validation = dag.get_task("validate_sources")
    assert validation.upstream_task_ids == {
        "extract_orders",
        "extract_payments",
        "extract_customers",
        "extract_products",
        "extract_stores",
    }


def test_both_audit_tasks_run_after_upstream_completion():
    bag = DagBag(include_examples=False)
    for dag_id in ["nammamart_etl_akshita", "nammamart_elt_akshita"]:
        dag = bag.get_dag(dag_id)
        assert dag.get_task("audit_run").trigger_rule == "all_done"


def test_etl_persists_queryable_rejection_counts_before_reporting():
    dag = DagBag(include_examples=False).get_dag("nammamart_etl_akshita")
    assert "persist_rejections" in dag.task_ids
    assert "persist_rejections" in dag.get_task("report").upstream_task_ids


def test_etl_serializes_writes_to_the_single_duckdb_file():
    dag = DagBag(include_examples=False).get_dag("nammamart_etl_akshita")
    assert "persist_dq_results" in dag.get_task("persist_rejections").upstream_task_ids
    assert "persist_rejections" in dag.get_task("load_star_schema").upstream_task_ids
    assert "load_star_schema" in dag.get_task("reconcile_payments").upstream_task_ids


def test_elt_freshness_check_ignores_future_order_timestamps():
    sql_path = Path(__file__).resolve().parents[2] / "include" / "sql" / "elt" / "08_dq_results.sql"
    sql = sql_path.read_text(encoding="utf-8")
    assert "MAX(order_ts) FILTER (WHERE order_ts <= CURRENT_TIMESTAMP)" in sql
