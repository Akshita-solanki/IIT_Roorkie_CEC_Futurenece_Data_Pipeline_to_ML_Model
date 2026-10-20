CREATE OR REPLACE TABLE mart_rejections_by_file_reason AS
WITH rejected AS (
    SELECT 'orders.csv' AS source_file, failure_reason FROM stg_orders WHERE dq_status = 'FAIL' AND _source_file = 'orders.csv'
    UNION ALL SELECT 'orders_2026-10-01_increment.csv', failure_reason FROM stg_orders WHERE dq_status = 'FAIL' AND _source_file = 'orders_2026-10-01_increment.csv'
    UNION ALL SELECT 'payments.csv', failure_reason FROM stg_payments WHERE dq_status = 'FAIL'
    UNION ALL SELECT 'customers.csv', failure_reason FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL SELECT 'products.csv', failure_reason FROM stg_products WHERE dq_status = 'FAIL'
    UNION ALL SELECT 'stores.csv', failure_reason FROM stg_stores WHERE dq_status = 'FAIL'
)
SELECT source_file, TRIM(reason.value) AS failure_reason, COUNT(*) AS rows_rejected
FROM rejected, UNNEST(STRING_SPLIT(COALESCE(failure_reason, ''), ';')) AS reason(value)
WHERE TRIM(reason.value) <> ''
GROUP BY source_file, TRIM(reason.value);
