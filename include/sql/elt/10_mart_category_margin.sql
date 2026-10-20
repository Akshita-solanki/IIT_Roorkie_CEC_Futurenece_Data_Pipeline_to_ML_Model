CREATE OR REPLACE TABLE mart_category_margin AS
SELECT p.category,
       ROUND(SUM(o.order_amount), 2) AS revenue,
       ROUND(SUM(o.order_amount - o.quantity * p.cost_price), 2) AS gross_margin,
       SUM(o.quantity) AS units_sold
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAIL'
WHERE o.order_status = 'DELIVERED' AND o.dq_status <> 'FAIL'
GROUP BY p.category;
