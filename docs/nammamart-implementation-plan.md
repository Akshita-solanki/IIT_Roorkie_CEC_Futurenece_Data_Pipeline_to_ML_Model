# NammaMart ETL and ELT Implementation Plan

> **For agentic workers:** Use the native execution approach in this session. Steps use checkbox syntax for tracking. Keep all changes uncommitted as requested.

**Goal:** Deliver two observable, idempotent NammaMart Airflow pipelines and the assignment evidence needed to demonstrate their behavior.

**Architecture:** Two scheduled TaskFlow DAGs read unchanged source copies and write to isolated DuckDB files. ETL validates in Python and quarantines critical failures before loading a masked star schema. ELT appends exact raw text records and uses SQL files to build quality-scored staging and business marts. Both write quality and run-audit records.

**Tech Stack:** Astro Runtime / Apache Airflow TaskFlow, Python, pandas, DuckDB, SQL, pendulum, Markdown.

**Spec:** [docs/nammamart-design.md](docs/nammamart-design.md)

## Global Constraints

- Do not modify `dags/meridian_pipeline.py`.
- Copy the six source CSVs unchanged into `include/data/nammamart/`; never edit the supplied source files by hand.
- Name the DAGs `nammamart_etl_akshita` and `nammamart_elt_akshita`.
- Use `include/warehouse_akshita_etl.duckdb` and `include/warehouse_akshita_elt.duckdb` for isolated writes.
- Schedule both DAGs at 09:00 Monday–Saturday in `Asia/Kolkata`, with `catchup=False`, retries, retry delay, and execution timeout.
- Mask customer email and phone in all reporting tables; preserve exact unmasked input only in the designated source/raw locations.
- Do not add secrets to DAGs or source control. Read configured credentials from Airflow Variables.
- Do not commit any changes.

## Review Focus

- Duplicate business keys in orders, payments, customers, and products; validation must quarantine or deterministically deduplicate according to table semantics.
- Increment rows can revise old order status; ELT staging merges all ten overlapping revisions while ETL applies only critical-check-passing corrections and quarantines invalid revisions.
- Null, malformed, and future timestamps must not silently become valid facts; freshness checks need a repeatable run clock.
- Payment joins can be one-to-many or missing; reconciliation must account for every mismatch without inflating revenue.
- Concurrent DAG writes must remain isolated; ETL and ELT totals must use identical delivered-order and discount rules.

---

### Task 1: Profile inputs and write the data issues log

**Files:**
- Create: `include/data/nammamart/` (six unchanged CSV copies)
- Create: `SUBMISSION.md` (Part 0 profile, issue log, and three client questions)

**Interfaces:**
- Consumes: six supplied CSV files in `assignment/nammamart_dataset/`.
- Produces: unchanged pipeline input copies and a row-level issue log with file, key, column, problem, dimension, proposed rule, and severity.

- [ ] Copy all six files without editing, then compare row counts and SHA-256 hashes with originals.
- [ ] Profile row counts, nulls, duplicate keys, numeric/date ranges, requested category values, four foreign-key relationships, and every stated business rule.
- [ ] Record every discovered issue with its exact source key and proposed resolution; include issues from all five primary files and profile the increment.
- [ ] Add three client clarification questions, including the treatment of negative quantity and whether stale orders should block a run.
- [ ] Confirm no original file content changed.

### Task 2: Add shared quality and privacy helpers

**Files:**
- Create: `include/nammamart_common.py`
- Create: `include/nammamart_quality.py`
- Create: `tests/test_nammamart_quality.py`

**Interfaces:**
- `normalize_text(value: object) -> object`
- `mask_email(value: object) -> object`
- `mask_phone(value: object) -> object`
- `validate_frame(file_name: str, frame: pandas.DataFrame, reference_frames: dict, run_at: datetime) -> ValidationResult`
- `ValidationResult` contains passed rows, rejected rows with `failure_reason`, and per-check counts for all six dimensions.

- [ ] Write focused tests for trim/casing normalization, UPI variants, email/phone masking, and one critical rule per quality dimension.
- [ ] Run the helper tests and confirm expected failures before implementation.
- [ ] Implement deterministic normalization, masking, and severity-aware validation with injectable `run_at` for timeliness.
- [ ] Run helper tests and confirm pass behavior, multiple failure reasons, and empty-input handling.

### Task 3: Build the ETL DAG

**Files:**
- Create: `dags/nammamart_etl_akshita.py`
- Create: `tests/dags/test_nammamart_etl_dag.py`
- Create at runtime: `include/quarantine/akshita/`
- Write: `include/warehouse_akshita_etl.duckdb`

**Interfaces:**
- Consumes: Task 1 source copies and Task 2 `ValidationResult`.
- Produces: `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`, `etl_payment_reconciliation`, `etl_daily_zone_revenue`, `etl_category_margin`, `dq_results`, and `pipeline_runs`.

- [ ] Define five independent extract tasks for orders, payments, customers, products, and stores; verify they are upstream of validation and can run in parallel.
- [ ] Add timezone-aware schedule `0 9 * * 1-6`, `Asia/Kolkata`, `catchup=False`, retries, retry delay, and execution timeout.
- [ ] Validate each source with shared helpers; write rejected records to a per-run CSV with one or more `failure_reason` values.
- [ ] Build masked customer, product, and store dimensions; join passing orders and derive order date, net revenue, gross margin, and within-15-minute flag.
- [ ] Upsert keyed dimensions and facts in a transaction; add the increment file as an upsert source so repeated runs do not change counts and corrections update existing orders.
- [ ] Reconcile clean orders against successful payments and build the two ETL reporting tables.
- [ ] Persist every check to `dq_results` and add an `all_done` audit task that writes `pipeline_runs` even after an upstream failure.
- [ ] Test DAG parsing, schedule metadata, critical-row exclusion, masked outputs, idempotent reruns, and failure audit behavior.

### Task 4: Build ELT raw and staging SQL

**Files:**
- Create: `dags/nammamart_elt_akshita.py`
- Create: `include/sql/elt/01_create_raw.sql`
- Create: `include/sql/elt/02_build_staging.sql`
- Create: `include/sql/elt/03_merge_increment.sql`
- Create: `tests/dags/test_nammamart_elt_dag.py`
- Write: `include/warehouse_akshita_elt.duckdb`

**Interfaces:**
- Produces raw tables `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores`, with all source columns as text plus `_loaded_at`, `_source_file`, `_run_id`.
- Produces typed `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores`; order staging is current-state keyed by `order_id` and has `dq_status`, `failure_reason`.

- [ ] Write SQL that creates append-only raw tables and imports each CSV as text without pandas transformations.
- [ ] Verify raw row counts equal source row counts after one run and grow by 160 rows after an increment run.
- [ ] Write SQL staging transformations for type casts, normalization, duplicate handling, PII masking, all six quality dimensions, and check result aggregation.
- [ ] Write order merge SQL that inserts 150 new orders and updates the ten overlapping order IDs.
- [ ] Add scheduled TaskFlow orchestration to run raw, staging, and increment SQL in dependency order, with the same schedule/retry policy as ETL.
- [ ] Persist `dq_results` and `pipeline_runs`, including all-done audit behavior.
- [ ] Test raw fidelity, malformed-row retention, staging quality metadata, merge insert/update counts, and DAG parse/schedule metadata.

### Task 5: Build ELT marts and business queries

**Files:**
- Create: `include/sql/elt/04_build_marts.sql`
- Create or modify: `SUBMISSION.md` (business query SQL and result sections)

**Interfaces:**
- Produces: `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, and `mart_top_customers`.

- [ ] Add SQL for daily revenue by zone/store and category revenue/margin, using only quality-passing delivered orders.
- [ ] Add delivery SLA, payment-mode/reconciliation, top-customer spend with loyalty tier, and rejection-count-by-file/reason marts or queries.
- [ ] Run the six business questions against ETL and ELT outputs and record SQL plus actual results.
- [ ] Compare September totals and document any remaining differences with exact rules and row keys; resolve unintended differences.

### Task 6: Complete governance, schedule, and client recommendation

**Files:**
- Modify: `SUBMISSION.md`

- [ ] Answer schedule questions with the cron expression, UTC display time, UTC start-date failure mode, and laptop/Docker-off behavior.
- [ ] Add concise ownership and change approval, dictionary, lineage, PII, applicable Indian law, access, retention, secrets, rejected-row audit, and late-change answers.
- [ ] Trace one mart metric to source file and row keys in both pipelines.
- [ ] Write a one-page recommendation addressed to Sameer, based on measured outputs and observed operational behavior; cover auditability, guarantees, change effort, 1000× growth, cost, and three risks.

### Task 7: Run observability experiments and capture evidence

**Files:**
- Modify: `SUBMISSION.md`
- Create: `docs/evidence/` screenshots captured from actual Airflow UI/query results

- [ ] For each DAG, run and record expected, observed, evidence location, and proposed change for happy path, rerun, increment, missing file, five bad rows, schema change, 50-row volume, price drift, stale data, and recovery.
- [ ] Restore the original input after every mutation experiment and verify source hashes at the end.
- [ ] Add a volume anomaly check if the 50-row experiment does not produce a warning.
- [ ] Map observed coverage to freshness, volume, schema, distribution, and lineage; identify strongest and weakest pillars.
- [ ] Capture both green DAG graphs, schedule, quarantine output, `dq_results`, `pipeline_runs`, and payment reconciliation query results.
- [ ] Run final DAG import checks and the relevant focused tests; confirm `git status` shows intended uncommitted files only and no commits were created.

