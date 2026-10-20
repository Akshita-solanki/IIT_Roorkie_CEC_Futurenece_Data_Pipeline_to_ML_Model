CREATE OR REPLACE TABLE stg_orders_source AS
WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER () AS _raw_row FROM raw_orders
), ranked AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY COALESCE(NULLIF(TRIM(order_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)), _run_id, _source_file) AS _duplicate_count,
           ROW_NUMBER() OVER (
               PARTITION BY COALESCE(NULLIF(TRIM(order_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR))
               ORDER BY _loaded_at DESC, CASE WHEN _source_file = 'orders_2026-10-01_increment.csv' THEN 1 ELSE 0 END DESC, _run_id DESC, _raw_row DESC
           ) AS _rank
    FROM numbered
), latest AS (SELECT * FROM ranked WHERE _rank = 1), casted AS (
    SELECT NULLIF(TRIM(order_id), '') AS order_id,
           TRY_CAST(NULLIF(TRIM(order_ts), '') AS TIMESTAMP) AS order_ts,
           NULLIF(TRIM(customer_id), '') AS customer_id,
           NULLIF(TRIM(store_id), '') AS store_id,
           NULLIF(TRIM(product_id), '') AS product_id,
           TRY_CAST(NULLIF(TRIM(quantity), '') AS INTEGER) AS quantity,
           TRY_CAST(NULLIF(TRIM(unit_price), '') AS DOUBLE) AS unit_price,
           TRY_CAST(NULLIF(TRIM(discount_pct), '') AS DOUBLE) AS discount_pct,
           TRY_CAST(NULLIF(TRIM(order_amount), '') AS DOUBLE) AS order_amount,
           CASE LOWER(TRIM(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cod' THEN 'Cash on Delivery' WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' ELSE TRIM(payment_mode) END AS payment_mode,
           CASE LOWER(TRIM(order_status)) WHEN 'delivered' THEN 'DELIVERED' WHEN 'cancelled' THEN 'CANCELLED' WHEN 'returned' THEN 'RETURNED' ELSE UPPER(TRIM(order_status)) END AS order_status,
           CASE LOWER(TRIM(channel))
               WHEN 'app' THEN 'App'
               WHEN 'web' THEN 'Web'
               ELSE TRIM(channel)
           END AS channel,
           TRY_CAST(NULLIF(TRIM(delivery_minutes), '') AS DOUBLE) AS delivery_minutes,
           _duplicate_count, _loaded_at, _source_file, _run_id
    FROM latest
), checked AS (
    SELECT c.*,
           p.mrp AS _product_mrp,
           (cu.customer_id IS NOT NULL AND cu.dq_status <> 'FAIL') AS _customer_valid,
           (p.product_id IS NOT NULL AND p.dq_status <> 'FAIL') AS _product_valid,
           (s.store_id IS NOT NULL AND s.dq_status <> 'FAIL') AS _store_valid,
           (c._duplicate_count > 1 OR c.order_id IS NULL OR c.order_ts IS NULL OR c.order_ts > CURRENT_TIMESTAMP OR c.customer_id IS NULL OR c.store_id IS NULL OR c.product_id IS NULL OR c.quantity IS NULL OR c.quantity <= 0 OR c.unit_price IS NULL OR c.discount_pct IS NULL OR c.discount_pct < 0 OR c.discount_pct > 100 OR c.order_amount IS NULL OR ABS(ROUND(c.quantity * c.unit_price * (1 - c.discount_pct / 100), 2) - c.order_amount) > 0.01 OR c.payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') OR c.order_status NOT IN ('DELIVERED','CANCELLED','RETURNED') OR (c.order_status = 'DELIVERED' AND c.delivery_minutes IS NULL) OR c.delivery_minutes >= 120 OR cu.customer_id IS NULL OR cu.dq_status = 'FAIL' OR p.product_id IS NULL OR p.dq_status = 'FAIL' OR s.store_id IS NULL OR s.dq_status = 'FAIL') AS _critical_failure,
           (p.mrp > 0 AND c.unit_price > p.mrp * 5) AS _price_drift,
           MAX(CASE WHEN c.order_ts <= CURRENT_TIMESTAMP THEN c.order_ts END) OVER () AS _latest_valid_order_ts
    FROM casted c
    LEFT JOIN stg_customers cu ON cu.customer_id = c.customer_id
    LEFT JOIN stg_products p ON p.product_id = c.product_id
    LEFT JOIN stg_stores s ON s.store_id = c.store_id
), classified AS (
    SELECT *,
           (_price_drift OR _latest_valid_order_ts < CURRENT_TIMESTAMP - INTERVAL '1 day') AS _warning_failure
    FROM checked
)
SELECT c.order_id, c.order_ts, c.customer_id, c.store_id, c.product_id, c.quantity, c.unit_price,
       c.discount_pct, c.order_amount, c.payment_mode, c.order_status, c.channel, c.delivery_minutes,
       CASE WHEN _critical_failure THEN 'FAIL' WHEN _warning_failure THEN 'WARN' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN _duplicate_count > 1 THEN 'duplicate order_id' END,
           CASE WHEN order_id IS NULL THEN 'missing order_id' END,
           CASE WHEN order_ts IS NULL THEN 'invalid order_ts' END,
           CASE WHEN order_ts > CURRENT_TIMESTAMP THEN 'future order_ts' END,
           CASE WHEN NOT _customer_valid THEN 'unknown or invalid customer_id' END,
           CASE WHEN NOT _store_valid THEN 'unknown or invalid store_id' END,
           CASE WHEN NOT _product_valid THEN 'unknown or invalid product_id' END,
           CASE WHEN quantity IS NULL OR quantity <= 0 THEN 'quantity must be positive' END,
           CASE WHEN unit_price IS NULL THEN 'invalid unit_price' END,
           CASE WHEN discount_pct IS NULL OR discount_pct < 0 OR discount_pct > 100 THEN 'discount_pct outside 0–100' END,
           CASE WHEN order_amount IS NULL OR ABS(ROUND(quantity * unit_price * (1 - discount_pct / 100), 2) - order_amount) > 0.01 THEN 'order amount formula mismatch' END,
           CASE WHEN payment_mode NOT IN ('UPI','Card','Cash on Delivery','Wallet') THEN 'invalid payment_mode' END,
           CASE WHEN order_status NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'invalid order_status' END,
           CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'delivered order missing delivery_minutes' END,
           CASE WHEN delivery_minutes >= 120 THEN 'delivery_minutes must be below 120' END,
           CASE WHEN _price_drift THEN 'unit_price exceeds 5x product MRP' END
       ), '') AS failure_reason,
       _loaded_at, _source_file, _run_id
FROM classified c;
