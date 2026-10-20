CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH clean_orders AS (
    SELECT * FROM stg_orders WHERE dq_status <> 'FAIL'
), all_orders AS (
    SELECT DISTINCT order_id FROM stg_orders WHERE order_id IS NOT NULL
), successful AS (
    SELECT * FROM stg_payments WHERE payment_status = 'SUCCESS'
), payment_counts AS (
    SELECT order_id, COUNT(*) AS success_count FROM successful GROUP BY order_id
), mismatches AS (
    SELECT o.order_id, p.payment_id, o.order_amount, p.amount_paid,
           ROUND(p.amount_paid - o.order_amount, 2) AS difference,
           'amount_mismatch' AS reason
    FROM clean_orders o JOIN successful p ON p.order_id = o.order_id
    JOIN payment_counts c ON c.order_id = p.order_id
    WHERE o.order_status = 'DELIVERED' AND c.success_count = 1
      AND (p.amount_paid IS NULL OR o.order_amount IS NULL OR ABS(p.amount_paid - o.order_amount) > 0.01)
    UNION ALL
    SELECT o.order_id, NULL, o.order_amount, NULL, NULL, 'missing_success_payment'
    FROM clean_orders o LEFT JOIN successful p ON p.order_id = o.order_id
    WHERE o.order_status = 'DELIVERED' AND p.payment_id IS NULL
    UNION ALL
    SELECT p.order_id, p.payment_id, NULL, p.amount_paid, NULL, 'unknown_order'
    FROM stg_payments p LEFT JOIN all_orders o ON o.order_id = p.order_id
    WHERE o.order_id IS NULL
    UNION ALL
    SELECT p.order_id, p.payment_id, o.order_amount, p.amount_paid,
           ROUND(p.amount_paid - o.order_amount, 2), 'successful_payment_for_non_delivered_order'
    FROM successful p JOIN clean_orders o ON o.order_id = p.order_id
    WHERE o.order_status <> 'DELIVERED'
    UNION ALL
    SELECT p.order_id, p.payment_id, o.order_amount, p.amount_paid,
           ROUND(p.amount_paid - o.order_amount, 2), 'multiple_success_payments'
    FROM successful p JOIN payment_counts c ON c.order_id = p.order_id
    LEFT JOIN clean_orders o ON o.order_id = p.order_id
    WHERE c.success_count > 1 AND o.order_status = 'DELIVERED'
)
SELECT * FROM mismatches;
