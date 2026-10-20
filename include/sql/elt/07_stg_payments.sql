CREATE OR REPLACE TABLE stg_payments AS
WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER () AS _raw_row FROM raw_payments
), ranked AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY COALESCE(NULLIF(TRIM(payment_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)), _run_id, _source_file) AS _duplicate_count,
           ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(TRIM(payment_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)) ORDER BY _loaded_at DESC, _raw_row DESC) AS _rank
    FROM numbered
), latest AS (SELECT * FROM ranked WHERE _rank = 1), casted AS (
    SELECT NULLIF(TRIM(payment_id), '') AS payment_id,
           NULLIF(TRIM(order_id), '') AS order_id,
           TRY_CAST(NULLIF(TRIM(payment_ts), '') AS TIMESTAMP) AS payment_ts,
           CASE LOWER(TRIM(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode) END AS payment_mode,
           TRY_CAST(NULLIF(TRIM(amount_paid), '') AS DOUBLE) AS amount_paid,
           UPPER(TRIM(payment_status)) AS payment_status,
           NULLIF(TRIM(gateway_ref), '') AS gateway_ref,
           _duplicate_count, _loaded_at, _source_file, _run_id
    FROM latest
), checked AS (
    SELECT p.*, o.order_amount AS _order_amount,
           (p._duplicate_count > 1 OR p.payment_id IS NULL OR p.order_id IS NULL OR p.payment_ts IS NULL OR p.payment_ts > CURRENT_TIMESTAMP OR p.payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') OR p.payment_status NOT IN ('SUCCESS','FAILED','REFUNDED') OR p.amount_paid IS NULL OR p.amount_paid < 0 OR (p.payment_status = 'SUCCESS' AND (o.order_id IS NULL OR ABS(p.amount_paid - o.order_amount) > 0.01))) AS _critical_failure,
           (p.gateway_ref IS NULL) AS _warning_failure
    FROM casted p
    LEFT JOIN stg_orders o ON o.order_id = p.order_id
), classified AS (
    SELECT *, CASE WHEN _critical_failure THEN 'FAIL' WHEN _warning_failure THEN 'WARN' ELSE 'PASS' END AS dq_status
    FROM checked
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref,
       dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN _duplicate_count > 1 THEN 'duplicate payment_id' END,
           CASE WHEN payment_id IS NULL THEN 'missing payment_id' END,
           CASE WHEN order_id IS NULL THEN 'missing order_id' END,
           CASE WHEN payment_ts IS NULL THEN 'invalid payment_ts' END,
           CASE WHEN payment_ts > CURRENT_TIMESTAMP THEN 'future payment_ts' END,
           CASE WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'invalid payment_mode' END,
           CASE WHEN payment_status NOT IN ('SUCCESS','FAILED','REFUNDED') THEN 'invalid payment_status' END,
           CASE WHEN amount_paid IS NULL OR amount_paid < 0 THEN 'amount_paid must be non-negative' END,
           CASE WHEN payment_status = 'SUCCESS' AND _order_amount IS NULL THEN 'payment references unknown order_id' END,
           CASE WHEN payment_status = 'SUCCESS' AND _order_amount IS NOT NULL AND ABS(amount_paid - _order_amount) > 0.01 THEN 'SUCCESS payment amount mismatch' END,
           CASE WHEN gateway_ref IS NULL THEN 'gateway_ref missing' END
       ), '') AS failure_reason,
       _loaded_at, _source_file, _run_id
FROM classified;
