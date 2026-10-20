# NammaMart ETL and ELT Assignment — Design

## Purpose

Replace NammaMart's late, manually joined morning spreadsheet with two observable Airflow pipelines that produce reconciled reporting data before the 9:30 AM IST operations review. The ETL path must keep invalid rows out of finance tables. The ELT path must preserve every source row for audit, then apply quality rules in DuckDB SQL. The deliverable compares both approaches and recommends one for NammaMart.

## Problem statement, broken down

1. **Reliable daily reporting:** run Monday through Saturday at 9:00 AM Asia/Kolkata, with retry, timeout, and run-history evidence.
2. **Correct revenue:** count delivered orders after discount, reject or quarantine critical failures, reconcile order amounts against successful gateway payments, and update late status corrections.
3. **Auditable data:** preserve raw ELT records exactly as text with source and run metadata; record each quality result; retain rejected ETL rows with reasons.
4. **Protected customer data:** mask email and phone in reporting tables, and document the restricted handling of raw CRM data.
5. **Useful business outputs:** publish answers for all six business questions from both pipelines.
6. **Demonstrable operations:** record run audits and the outcome of the ten requested observability experiments for both DAGs.
7. **Client decision:** document governance answers, data lineage, the ETL/ELT trade-offs, and a recommendation to the CTO.

## Existing project and constraints

- This is an initialized Astro/Airflow repository. `dags/meridian_pipeline.py` is the trainer's demo and must remain unchanged.
- The six source CSVs are under `assignment/nammamart_dataset/`. Copy them unchanged to `include/data/nammamart/` before pipeline runs.
- Use separate DAGs named `nammamart_etl_akshita` and `nammamart_elt_akshita`.
- Use isolated DuckDB files, `include/warehouse_akshita_etl.duckdb` and `include/warehouse_akshita_elt.duckdb`, so both 9:00 DAGs can write without contending on one file.
- Existing project dependencies include pandas and DuckDB. Avoid adding dependencies unless implementation proves one is necessary.
- Keep all changes uncommitted, as requested.

## Discovery findings

The assignment's supplied counts total 5,372 rows across six files. A first-pass profile found defects in every primary source file:

| Source | Initial findings |
| --- | --- |
| `orders.csv` (2,412 rows) | 24 rows participate in duplicate `order_id` keys; 18 missing `customer_id`; 2 missing timestamps and 4 non-null timestamps that fail parsing; 7 future timestamps; 18 order amount formula mismatches; 10 non-positive quantities; 3 discounts outside 0–100; 8 delivered orders without delivery minutes; 6 delivery values at or above 120 minutes; 3 invalid statuses; 2 invalid payment modes after alias/case normalization; 9 unknown product and 5 unknown store references. Other case/spacing variants normalize to allowed modes. |
| `payments.csv` (2,228 rows) | 8 rows participate in duplicate `payment_id` keys; 301 missing `gateway_ref`; 8 payment rows reference unknown orders; 14 successful payment amounts differ from the associated order amount. |
| `customers.csv` (405 rows) | 10 rows participate in duplicate `customer_id` keys; 4 missing emails; 6 missing cities; loyalty tiers use inconsistent casing and spacing. |
| `products.csv` (152 rows) | 4 rows participate in duplicate `product_id` keys; 3 missing categories; `mrp` ranges down to -203. |
| `stores.csv` (15 rows) | 1 missing zone; capacity ranges down to zero. |
| `orders_2026-10-01_increment.csv` (160 rows) | 10 IDs overlap September orders and represent corrections; 150 are new. This is the incremental upsert input. |

These are initial aggregate findings, not the final row-level Data Issues Log. Part 0 must retain the exact row key, column, observed issue, dimension, proposed rule, and severity for each issue. Profiling uses source copies and must not edit the supplied CSVs.

## Architecture

### Shared source and scheduling

Both DAGs read the same five September source files and the optional October 1 increment file from `include/data/nammamart/`. Five independent extract tasks fan out in parallel. Each DAG uses an explicit `Asia/Kolkata` timezone-aware start date, the Monday–Saturday 09:00 cron schedule, `catchup=False`, retries, a retry delay, and an execution timeout.

### ETL path

`Extract → Validate → Quarantine → Transform → Load → Reconcile → Report`

- Python helpers normalize values and evaluate critical and warning rules across completeness, uniqueness, validity, accuracy, consistency, and timeliness.
- Critical failures are retained in per-run quarantine CSVs with `failure_reason`; only passing records enter the finance star schema.
- Transform joins orders to customer, product, and store dimensions; masks customer email and phone; and derives order date, net revenue, gross margin, and delivery SLA fields.
- DuckDB loads are idempotent. The increment feed upserts order IDs so late status changes replace prior current-state values without duplicate facts.
- Reconciliation records every order/payment mismatch with a reason. Daily zone/store revenue and category margin are built and printed.

### ELT path

`Extract → Load raw → Transform staging SQL → Build mart SQL → Report`

- Raw tables store every CSV cell as text, including malformed rows, plus `_loaded_at`, `_source_file`, and `_run_id`. Raw rows are append-only.
- SQL files under `include/sql/` create typed and standardized staging tables, mask PII, deduplicate current records, attach `dq_status` and `failure_reason`, and perform the incremental order merge by `order_id`.
- SQL mart tables answer the business questions. The September results must match ETL under the same inclusion and revenue rules.

### Shared audit and quality records

Each DAG writes `dq_results` on every run with the required check metadata and uses an `all_done` final audit task to write one `pipeline_runs` row, including success or failure status and row counts. The audit task must still run when an upstream task fails.

### Submission evidence

`SUBMISSION.md` will contain the final data issues log, client questions, scheduling answers, data dictionary, governance responses, business query SQL and results, observability experiment table, observability-pillar map, and the one-page CTO recommendation. Screenshots requested by the assignment will be saved under `docs/evidence/` only when captured from actual Airflow runs and queries.

## Key decisions and assumptions

- **Current state after corrections:** ELT staging merges all ten overlapping correction rows by `order_id`, including any rows marked `FAIL`; marts exclude failed rows. ETL quarantines corrections that fail critical checks and applies only passing changes to finance facts. Initial validation shows three of the ten overlaps reference invalid master keys, so those three cannot both enter ETL facts and satisfy the no-bad-orders rule. Raw ELT history keeps each original and correction row. If finance needs historical as-was reporting, preserve an order status history table or snapshot rather than rewriting raw data.
- **Severity:** critical failures block ETL reporting rows and are marked failed in ELT staging; warning issues remain loadable and are visible in `dq_results`.
- **PII:** customer `full_name`, `email`, and `phone` are personal data. Reporting outputs mask email and phone, and only the controlled customer dimension may carry a masked name if required by business questions.
- **Date horizon:** profile rules should treat future timestamps as critical and stale data as a warning. A fixed test clock or documented run date is needed for repeatable freshness experiment evidence.
- **Source paths:** the assignment's originals stay untouched; pipeline execution uses their unchanged copies under `include/data/nammamart/`.
- **Observability claims:** distinguish static/code-level expectations from outcomes actually observed in the Airflow UI, task logs, and DuckDB. Do not claim screenshots or experiment observations until they exist.

## Acceptance mapping

| Requirement | Design element |
| --- | --- |
| 9:00 AM IST Monday–Saturday | Both DAG cron schedules, timezone-aware start date, schedule evidence, and Part C explanations |
| No bad order in finance | ETL critical validation gates fact loading; rejected rows are quarantined |
| Exact raw history | ELT append-only all-text raw tables with source/run metadata |
| Rejection reason | ETL quarantine `failure_reason`; ELT staging `dq_status` and `failure_reason` |
| Idempotency | ETL keyed upsert; ELT raw append semantics plus staging merge keyed by `order_id` |
| Incremental correction | October feed appends raw rows and updates current staging/fact rows for ten overlaps |
| Payment reconciliation | Pipeline-specific reconciliation tables with a row and reason per mismatch |
| Masked PII | Masking before ETL reporting load and inside ELT staging/marts |
| Run health and reason | `pipeline_runs`, `dq_results`, Airflow history and logs |
| Client recommendation | Evidence-based Part G in `SUBMISSION.md` |
| Six business questions | Query and result section for both ETL and ELT marts |
| Ten observability experiments | Experiment matrix executed against each DAG and recorded with expected/observed/evidence/change |

