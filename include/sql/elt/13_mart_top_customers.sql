CREATE OR REPLACE TABLE mart_top_customers AS
SELECT o.customer_id, c.loyalty_tier,
       ROUND(SUM(o.order_amount), 2) AS spend,
       COUNT(*) AS delivered_orders
FROM stg_orders o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAIL'
WHERE o.order_status = 'DELIVERED' AND o.dq_status <> 'FAIL'
GROUP BY o.customer_id, c.loyalty_tier
ORDER BY spend DESC, o.customer_id
LIMIT 10;
