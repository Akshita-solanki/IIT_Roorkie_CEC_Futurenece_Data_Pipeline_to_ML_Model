CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT CAST(o.order_ts AS DATE) AS order_date,
       s.zone, s.store_id, s.store_name,
       COUNT(*) AS delivered_orders,
       ROUND(SUM(o.order_amount), 2) AS net_revenue
FROM stg_orders o
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAIL'
WHERE o.order_status = 'DELIVERED' AND o.dq_status <> 'FAIL'
GROUP BY CAST(o.order_ts AS DATE), s.zone, s.store_id, s.store_name;
