# Submission evidence checklist

## Included

- Successful ETL and ELT DAG graph images.
- Schedule and next-run screenshots for both DAGs (`schedule-etl.png`, `schedule-elt.png`).
- ETL quarantine task log showing generated quarantine file paths and row counts (`etl-quarantine-task-log.png`).
- ETL product quarantine CSV screenshot without customer data (`etl-quarantine-file.png`).
- ETL and ELT DQ results screenshots (`dq-results-etl.png`, `dq-results-elt.png`).
- ETL and ELT pipeline run history screenshots (`pipeline-runs-etl.png`, `pipeline-runs-elt.png`).
- Reconciliation summary screenshots for ETL and ELT and an ETL detail screenshot (`payment-reconciliation-summary-*.png`, `payment-reconciliation-detail-etl.png`).
- Six business-query result CSVs for each pipeline.
- ETL and ELT `dq_results` extracts for the manually verified runs.
- ETL and ELT `pipeline_runs` extracts for the manually verified runs.
- Full payment-reconciliation detail and grouped totals from both pipelines.
- `final-run-metrics.json` and `observability-experiments.json` for the final runs and ten experiments.

The manually verified run IDs used by the query CSVs are `manual__2026-10-19T19:01:09.921049+00:00` (ETL) and `manual__2026-10-19T19:01:18.024800+00:00` (ELT). Both completed successfully. The Airflow schedule screenshots show a later successful run at 09:00 IST on 2026-10-20; the query CSVs and quarantine task log remain tied to the manual runs above.

## Remaining polish

The assignment evidence checklist is covered. The product quarantine screenshot shows no customer information; several long product names and failure reasons are clipped at the current Excel column widths. For a more readable capture, widen columns B and K and recapture `etl-quarantine-file.png`.

The ELT reconciliation detail CSV is also available under `query-results/`; its screenshot is optional because the ETL detail screenshot and both pipeline summaries are present. The CSV exports remain the complete tabular evidence, including all 211 reconciliation records.
