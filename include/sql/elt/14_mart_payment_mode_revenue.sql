CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
SELECT payment_mode, ROUND(SUM(order_amount), 2) AS net_revenue,
       COUNT(*) AS delivered_orders
FROM stg_orders
WHERE order_status = 'DELIVERED' AND dq_status <> 'FAIL'
GROUP BY payment_mode;
