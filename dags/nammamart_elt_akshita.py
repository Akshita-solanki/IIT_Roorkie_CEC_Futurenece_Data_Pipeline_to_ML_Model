"""NammaMart SQL ELT pipeline: raw history, quality staging, business marts."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path
import re
import sys

import duckdb
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_common import pipeline_status_from_task_states, validate_csv_headers


DATA_DIR = Path("/usr/local/airflow/include/data/nammamart")
SQL_DIR = Path("/usr/local/airflow/include/sql/elt")
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_akshita_elt.duckdb"
INCREMENT_FILE = "orders_2026-10-01_increment.csv"
BASE_FILES = {
    "orders.csv": "raw_orders",
    "payments.csv": "raw_payments",
    "customers.csv": "raw_customers",
    "products.csv": "raw_products",
    "stores.csv": "raw_stores",
}
REQUIRED_COLUMNS = {
    "orders.csv": {"order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"},
    "payments.csv": {"payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"},
    "customers.csv": {"customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"},
    "products.csv": {"product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"},
    "stores.csv": {"store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"},
}
DEFAULT_ARGS = {
    "owner": "akshita",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _escape_sql(value: str) -> str:
    return value.replace("'", "''")


def _execute_file(con, filename: str, substitutions: dict[str, str] | None = None) -> None:
    sql = (SQL_DIR / filename).read_text(encoding="utf-8")
    for key, value in (substitutions or {}).items():
        sql = sql.replace("{{" + key + "}}", _escape_sql(value))
    if sql.strip():
        con.execute(sql)


@dag(
    dag_id="nammamart_elt_akshita",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "elt", "assignment"],
    doc_md=__doc__,
)
def nammamart_elt_akshita():
    @task
    def extract_sources() -> dict:
        files = dict(BASE_FILES)
        increment = DATA_DIR / INCREMENT_FILE
        if increment.exists():
            files[INCREMENT_FILE] = "raw_orders"
        missing = [filename for filename in BASE_FILES if not (DATA_DIR / filename).exists()]
        if missing:
            raise FileNotFoundError(f"Missing required NammaMart source files: {', '.join(missing)}")
        for filename, required_columns in REQUIRED_COLUMNS.items():
            validate_csv_headers(DATA_DIR / filename, required_columns)
        print(f"[Extract] source files={list(files)}")
        return files

    @task
    def load_raw(files: dict) -> dict:
        from airflow.sdk import get_current_context

        context = get_current_context()
        run_id = context["run_id"]
        os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
        con = duckdb.connect(WAREHOUSE_DB)
        row_counts = {}
        for filename, table in files.items():
            path = str(DATA_DIR / filename)
            _execute_file(con, "01_load_raw.sql", {
                "table_name": table,
                "path": path,
                "source_file": filename,
                "run_id": run_id,
            })
            row_counts[filename] = int(con.execute(
                f"SELECT COUNT(*) FROM read_csv('{_escape_sql(path)}', header=true, all_varchar=true)"
            ).fetchone()[0])
            print(f"[Raw] appended {row_counts[filename]} rows from {filename} into {table}")
        con.close()
        return {"rows_extracted": sum(row_counts.values()), "row_counts": row_counts}

    @task
    def transform_staging(files: dict) -> dict:
        from airflow.sdk import get_current_context

        context = get_current_context()
        run_id = context["run_id"]
        con = duckdb.connect(WAREHOUSE_DB)
        base_keys = set()
        increment_rows = 0
        increment_inserted = 0
        increment_updated = 0
        if INCREMENT_FILE in files:
            base_keys = {
                row[0] for row in con.execute(
                    "SELECT DISTINCT order_id FROM raw_orders WHERE _run_id=? AND _source_file='orders.csv' AND order_id IS NOT NULL",
                    [run_id],
                ).fetchall()
            }
            increment_ids = [row[0] for row in con.execute(
                "SELECT DISTINCT NULLIF(TRIM(order_id), '') FROM raw_orders WHERE _run_id=? AND _source_file=? AND NULLIF(TRIM(order_id), '') IS NOT NULL",
                [run_id, INCREMENT_FILE],
            ).fetchall()]
            increment_rows = len(increment_ids)
            increment_inserted = sum(order_id not in base_keys for order_id in increment_ids)
            increment_updated = sum(order_id in base_keys for order_id in increment_ids)

        for sql_file in [
            "02_stg_customers.sql",
            "03_stg_products.sql",
            "04_stg_stores.sql",
            "05_stg_orders_source.sql",
            "06_merge_orders.sql",
            "07_stg_payments.sql",
        ]:
            _execute_file(con, sql_file)
            print(f"[Staging SQL] {sql_file} complete")
        staged = int(con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0])
        failed = int(con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status='FAIL'").fetchone()[0])
        print(f"[Staging] current order keys={staged}; critical failures={failed}; increment rows={increment_rows}, inserted={increment_inserted}, updated={increment_updated}")
        con.close()
        return {
            "staged_orders": staged,
            "failed_orders": failed,
            "increment_rows": increment_rows,
            "increment_inserted": increment_inserted,
            "increment_updated": increment_updated,
        }

    @task
    def persist_dq_results() -> int:
        from airflow.sdk import get_current_context

        context = get_current_context()
        con = duckdb.connect(WAREHOUSE_DB)
        before = con.execute("SELECT COUNT(*) FROM dq_results").fetchone()[0] if con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='dq_results'"
        ).fetchone()[0] else 0
        _execute_file(con, "08_dq_results.sql", {"run_id": context["run_id"]})
        after = int(con.execute("SELECT COUNT(*) FROM dq_results").fetchone()[0])
        con.close()
        return after - int(before)

    @task
    def build_marts(dq_rows: int) -> dict:
        con = duckdb.connect(WAREHOUSE_DB)
        mart_files = [
            "09_mart_daily_store_kpis.sql",
            "10_mart_category_margin.sql",
            "11_mart_delivery_sla.sql",
            "12_mart_payment_reconciliation.sql",
            "13_mart_top_customers.sql",
            "14_mart_payment_mode_revenue.sql",
            "15_mart_rejections_by_reason.sql",
        ]
        counts = {}
        tables = [
            "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
            "mart_payment_reconciliation", "mart_top_customers",
            "mart_payment_mode_revenue", "mart_rejections_by_file_reason",
        ]
        for filename in mart_files:
            _execute_file(con, filename)
        for table in tables:
            counts[table] = int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            print(f"[Mart] {table}: {counts[table]} rows")
            print(con.execute(f"SELECT * FROM {table} LIMIT 20").fetchdf().to_string(index=False))
        con.close()
        return {"mart_rows": counts, "dq_rows": dq_rows}

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run() -> None:
        from airflow.sdk import get_current_context

        context = get_current_context()
        ti = context["ti"]
        raw_result = ti.xcom_pull(task_ids="load_raw") or {}
        os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
        )""")
        has_orders = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name='stg_orders'").fetchone()[0] > 0
        loaded = int(con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status <> 'FAIL'").fetchone()[0]) if has_orders else 0
        rejected = int(con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'FAIL'").fetchone()[0]) if has_orders else 0
        task_states = ti.get_task_states(
            dag_id=context["dag"].dag_id,
            run_ids=[context["run_id"]],
        )
        states_for_run = task_states.get(context["run_id"], {})
        status = pipeline_status_from_task_states(states_for_run)
        start = context["dag_run"].start_date or pendulum.now("UTC")
        end = pendulum.now("UTC")
        con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [context["run_id"], context["dag"].dag_id])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
            context["run_id"], context["dag"].dag_id, start, end,
            int(raw_result.get("rows_extracted", 0)), loaded, rejected, status,
        ])
        con.close()
        print(f"[Audit] {context['dag'].dag_id} run={context['run_id']} status={status} extracted={raw_result.get('rows_extracted', 0)} loaded={loaded} rejected={rejected}")
        if status == "FAILED":
            raise AirflowException("NammaMart ELT run has failed or upstream-failed tasks; see task states and pipeline_runs audit row.")

    sources = extract_sources()
    raw = load_raw(sources)
    staging = transform_staging(sources)
    dq_rows = persist_dq_results()
    marts = build_marts(dq_rows)
    audit = audit_run()
    marts >> audit
    raw >> staging
    staging >> dq_rows


nammamart_elt_akshita()
