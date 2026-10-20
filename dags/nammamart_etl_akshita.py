"""NammaMart Python ETL pipeline: Extract, Validate, Transform, Load."""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/usr/local/airflow/include")

from nammamart_common import normalize_frame, pipeline_status_from_task_states, upsert_orders
from nammamart_etl import build_payment_reconciliation, build_reporting_frames
from nammamart_quality import validate_frame


DATA_DIR = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_akshita_etl.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/akshita"
SOURCE_FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]
INCREMENT_FILE = "orders_2026-10-01_increment.csv"

DEFAULT_ARGS = {
    "owner": "akshita",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}


def _records_json(frame: pd.DataFrame) -> str:
    return frame.to_json(orient="records", date_format="iso")


def _frame(payload: str | None) -> pd.DataFrame:
    return pd.DataFrame(json.loads(payload or "[]"))


@dag(
    dag_id="nammamart_etl_akshita",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["nammamart", "etl", "assignment"],
    doc_md=__doc__,
)
def nammamart_etl_akshita():
    @task
    def extract_orders() -> dict:
        base = pd.read_csv(f"{DATA_DIR}/orders.csv")
        increment_path = Path(DATA_DIR) / INCREMENT_FILE
        increment = pd.read_csv(increment_path) if increment_path.exists() else pd.DataFrame()
        print(f"[Extract] orders={len(base)} increment={len(increment)}")
        return {"records": _records_json(base), "increment": _records_json(increment), "rows": len(base) + len(increment)}

    @task
    def extract_payments() -> dict:
        frame = pd.read_csv(f"{DATA_DIR}/payments.csv")
        print(f"[Extract] payments={len(frame)}")
        return {"records": _records_json(frame), "rows": len(frame)}

    @task
    def extract_customers() -> dict:
        frame = pd.read_csv(f"{DATA_DIR}/customers.csv")
        print(f"[Extract] customers={len(frame)}")
        return {"records": _records_json(frame), "rows": len(frame)}

    @task
    def extract_products() -> dict:
        frame = pd.read_csv(f"{DATA_DIR}/products.csv")
        print(f"[Extract] products={len(frame)}")
        return {"records": _records_json(frame), "rows": len(frame)}

    @task
    def extract_stores() -> dict:
        frame = pd.read_csv(f"{DATA_DIR}/stores.csv")
        print(f"[Extract] stores={len(frame)}")
        return {"records": _records_json(frame), "rows": len(frame)}

    @task
    def validate_sources(orders_payload, payments_payload, customers_payload, products_payload, stores_payload) -> dict:
        orders = _frame(orders_payload["records"])
        increment = _frame(orders_payload.get("increment"))
        payments = _frame(payments_payload["records"])
        customers = _frame(customers_payload["records"])
        products = _frame(products_payload["records"])
        stores = _frame(stores_payload["records"])
        run_at = pendulum.now("Asia/Kolkata")
        validations = {}
        validations["customers.csv"] = validate_frame("customers.csv", customers, {}, run_at)
        validations["products.csv"] = validate_frame("products.csv", products, {}, run_at)
        validations["stores.csv"] = validate_frame("stores.csv", stores, {}, run_at)
        references = {
            "customers": validations["customers.csv"].passed,
            "products": validations["products.csv"].passed,
            "stores": validations["stores.csv"].passed,
        }
        validations["orders.csv"] = validate_frame("orders.csv", orders, references, run_at)
        if not increment.empty:
            validations[INCREMENT_FILE] = validate_frame(INCREMENT_FILE, increment, references, run_at)
        payment_references = {"orders": validations["orders.csv"].passed}
        validations["payments.csv"] = validate_frame("payments.csv", payments, payment_references, run_at)
        serialized = {}
        for filename, result in validations.items():
            serialized[filename] = {
                "passed": _records_json(result.passed.drop(columns=["_dq_warning"], errors="ignore")),
                "failed": _records_json(result.failed),
                "checks": result.checks,
                "rows_in": len(result.passed) + len(result.failed),
                "rows_passed": len(result.passed),
                "rows_failed": len(result.failed),
            }
            print(f"[Validate] {filename}: passed={len(result.passed)} failed={len(result.failed)}")
        return {
            "files": serialized,
            "raw_orders": _records_json(pd.concat([orders, increment], ignore_index=True) if not increment.empty else orders),
            "raw_payments": _records_json(payments),
            "rows_extracted": int(sum(item["rows"] for item in [orders_payload, payments_payload, customers_payload, products_payload, stores_payload])),
        }

    @task
    def quarantine_failures(validation: dict) -> dict:
        from airflow.sdk import get_current_context

        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(get_current_context()["run_id"]))
        counts = {}
        for filename, result in validation["files"].items():
            failed = _frame(result["failed"])
            counts[filename] = len(failed)
            if failed.empty:
                continue
            stem = filename.removesuffix(".csv")
            target = Path(QUARANTINE_DIR) / f"{stem}_failed_{run_id}.csv"
            failed.to_csv(target, index=False)
            print(f"[Quarantine] {len(failed)} rows → {target}")
        return counts

    @task
    def persist_dq_results(validation: dict) -> int:
        from airflow.sdk import get_current_context

        context = get_current_context()
        run_id = context["run_id"]
        rows = []
        for file_results in validation["files"].values():
            for check in file_results["checks"]:
                rows.append({
                    "run_id": run_id,
                    "dag_id": context["dag"].dag_id,
                    "check_name": check["check_name"],
                    "dimension": check["dimension"],
                    "severity": check["severity"],
                    "rows_checked": check["rows_checked"],
                    "rows_failed": check["rows_failed"],
                    "status": check["status"],
                    "checked_at": check["checked_at"],
                })
        os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
            checked_at TIMESTAMP
        )""")
        con.execute("DELETE FROM dq_results WHERE run_id=? AND dag_id=?", [run_id, context["dag"].dag_id])
        if rows:
            dq = pd.DataFrame(rows)
            con.register("dq_batch", dq)
            con.execute("INSERT INTO dq_results SELECT * FROM dq_batch")
            con.unregister("dq_batch")
        con.close()
        return len(rows)

    @task
    def persist_rejections(validation: dict, dq_count: int) -> int:
        from collections import Counter
        from airflow.sdk import get_current_context

        context = get_current_context()
        counts = Counter()
        for filename, result in validation["files"].items():
            failed = _frame(result["failed"])
            if "failure_reason" not in failed:
                continue
            for reasons in failed["failure_reason"].dropna():
                for reason in str(reasons).split(";"):
                    reason = reason.strip()
                    if reason:
                        counts[(filename, reason)] += 1

        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS etl_rejections_by_file_reason (
            run_id VARCHAR, source_file VARCHAR, failure_reason VARCHAR, rows_rejected BIGINT
        )""")
        con.execute("DELETE FROM etl_rejections_by_file_reason WHERE run_id=?", [context["run_id"]])
        if counts:
            rows = [(context["run_id"], filename, reason, count)
                    for (filename, reason), count in sorted(counts.items())]
            con.executemany("INSERT INTO etl_rejections_by_file_reason VALUES (?, ?, ?, ?)", rows)
        con.close()
        print(f"[Rejections] {sum(counts.values())} row/reason findings across {len(counts)} file/reason groups")
        return sum(counts.values())

    @task
    def transform_passed_rows(validation: dict) -> dict:
        files = validation["files"]
        orders = _frame(files["orders.csv"]["passed"])
        customers = _frame(files["customers.csv"]["passed"])
        products = _frame(files["products.csv"]["passed"])
        stores = _frame(files["stores.csv"]["passed"])
        increment = _frame(files[INCREMENT_FILE]["passed"]) if INCREMENT_FILE in files else pd.DataFrame()
        merged_orders, inserted, updated = upsert_orders(orders, increment) if not increment.empty else (orders, 0, 0)
        frames = build_reporting_frames(merged_orders, customers, products, stores)
        print(f"[Transform] clean fact rows={len(frames['etl_fact_orders'])}; increment inserted={inserted}, updated={updated}")
        return {name: _records_json(frame) for name, frame in frames.items()} | {
            "increment_inserted": inserted,
            "increment_updated": updated,
        }

    @task
    def load_star_schema(
        transformed: dict, quarantine_counts: dict, dq_count: int, rejection_count: int
    ) -> dict:
        os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
        con = duckdb.connect(WAREHOUSE_DB)
        table_rows = {}
        for table in ["etl_dim_customer", "etl_dim_product", "etl_dim_store", "etl_fact_orders"]:
            frame = _frame(transformed[table])
            con.register("load_batch", frame)
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM load_batch")
            con.unregister("load_batch")
            table_rows[table] = len(frame)
        con.close()
        quarantined = int(sum(quarantine_counts.values()))
        loaded = table_rows["etl_fact_orders"]
        print(f"[Load] idempotent snapshot loaded: {loaded} facts; quarantined={quarantined}; dq_checks={dq_count}")
        return {
            "rows_loaded": loaded,
            "rows_quarantined": quarantined,
            "table_rows": table_rows,
            "increment_inserted": transformed["increment_inserted"],
            "increment_updated": transformed["increment_updated"],
        }

    @task
    def reconcile_payments(validation: dict, load_result: dict) -> int:
        # Reconcile only the same critical-check-passing order population that
        # is eligible for the finance fact table. Keep raw payments so a payment
        # for a rejected/unknown order remains visible as an orphan exception.
        orders = _frame(validation["files"]["orders.csv"]["passed"])
        if INCREMENT_FILE in validation["files"]:
            increment = _frame(validation["files"][INCREMENT_FILE]["passed"])
            orders, _, _ = upsert_orders(orders, increment)
        payments = _frame(validation["raw_payments"])
        all_orders = _frame(validation["raw_orders"])
        reconciliation = build_payment_reconciliation(orders, payments, all_orders)
        con = duckdb.connect(WAREHOUSE_DB)
        con.register("reconciliation_batch", reconciliation)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM reconciliation_batch")
        con.unregister("reconciliation_batch")
        con.close()
        print(f"[Reconcile] mismatch rows={len(reconciliation)}")
        return len(reconciliation)

    @task
    def report(load_result: dict, reconciliation_count: int, rejection_count: int) -> None:
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, zone, store_id, store_name,
                   ROUND(SUM(net_revenue), 2) AS net_revenue,
                   COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY order_date, zone, store_id, store_name
        """)
        con.execute("""CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT category, ROUND(SUM(net_revenue), 2) AS revenue,
                   ROUND(SUM(gross_margin), 2) AS gross_margin
            FROM etl_fact_orders
            WHERE order_status = 'DELIVERED'
            GROUP BY category
        """)
        print("[Report] Daily net revenue by zone/store:")
        print(con.execute("SELECT * FROM etl_daily_zone_revenue ORDER BY order_date, zone, store_id").fetchdf().to_string(index=False))
        print("[Report] Revenue and margin by category:")
        print(con.execute("SELECT * FROM etl_category_margin ORDER BY category").fetchdf().to_string(index=False))
        con.close()
        print(f"[Report] reconciliation mismatch rows={reconciliation_count}; fact rows={load_result['rows_loaded']}")
        print(f"[Report] rejection row/reason findings={rejection_count}")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def audit_run() -> None:
        from airflow.sdk import get_current_context

        context = get_current_context()
        ti = context["ti"]
        validation = ti.xcom_pull(task_ids="validate_sources") or {}
        load_result = ti.xcom_pull(task_ids="load_star_schema") or {}
        extracted = int(validation.get("rows_extracted", 0))
        quarantined = int(load_result.get("rows_quarantined", 0))
        task_states = ti.get_task_states(
            dag_id=context["dag"].dag_id,
            run_ids=[context["run_id"]],
        )
        states_for_run = task_states.get(context["run_id"], {})
        status = pipeline_status_from_task_states(states_for_run)
        start = context["dag_run"].start_date or pendulum.now("UTC")
        end = pendulum.now("UTC")
        os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
        con = duckdb.connect(WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR
        )""")
        con.execute("DELETE FROM pipeline_runs WHERE run_id=? AND dag_id=?", [context["run_id"], context["dag"].dag_id])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
            context["run_id"], context["dag"].dag_id, start, end,
            extracted, int(load_result.get("rows_loaded", 0)), quarantined, status,
        ])
        con.close()
        print(f"[Audit] {context['dag'].dag_id} run={context['run_id']} status={status} extracted={extracted} loaded={load_result.get('rows_loaded', 0)} quarantined={quarantined}")
        if status == "FAILED":
            raise AirflowException("NammaMart ETL run has failed or upstream-failed tasks; see task states and pipeline_runs audit row.")

    orders = extract_orders()
    payments = extract_payments()
    customers = extract_customers()
    products = extract_products()
    stores = extract_stores()
    validated = validate_sources(orders, payments, customers, products, stores)
    quarantine = quarantine_failures(validated)
    dq_rows = persist_dq_results(validated)
    rejection_rows = persist_rejections(validated, dq_rows)
    transformed = transform_passed_rows(validated)
    loaded = load_star_schema(transformed, quarantine, dq_rows, rejection_rows)
    reconciled = reconcile_payments(validated, loaded)
    reported = report(loaded, reconciled, rejection_rows)
    audit = audit_run()
    reported >> audit


nammamart_etl_akshita()
