CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT s.store_id, s.store_name, s.zone,
       ROUND(AVG(o.delivery_minutes), 2) AS average_delivery_minutes,
       ROUND(100.0 * COUNT(*) FILTER (WHERE o.delivery_minutes <= 15) / NULLIF(COUNT(*), 0), 2) AS delivered_within_15_pct,
       COUNT(*) AS delivered_orders
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAIL'
WHERE o.order_status = 'DELIVERED' AND o.dq_status <> 'FAIL' AND o.delivery_minutes IS NOT NULL
GROUP BY s.store_id, s.store_name, s.zone;
