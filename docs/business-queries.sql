-- Run the ETL and ELT versions against their corresponding DuckDB warehouse.

-- Q1: daily net revenue by zone and store (ETL)
SELECT order_date, zone, store_id, store_name,
       SUM(net_revenue) AS net_revenue,
       COUNT(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date, zone, store_id;

-- Q1: daily net revenue by zone and store (ELT)
SELECT order_date, zone, store_id, store_name,
       SUM(net_revenue) AS net_revenue, SUM(delivered_orders) AS delivered_orders
FROM mart_daily_store_kpis
GROUP BY order_date, zone, store_id, store_name
ORDER BY order_date, zone, store_id;

-- Q2: revenue and gross margin by category (ETL / ELT)
SELECT category, revenue, gross_margin FROM etl_category_margin ORDER BY category;
SELECT category, revenue, gross_margin FROM mart_category_margin ORDER BY category;

-- Q3: delivery time and percentage within 15 minutes by store (ETL)
SELECT store_id, store_name, AVG(delivery_minutes) AS average_delivery_minutes,
       100.0 * COUNT(*) FILTER (WHERE within_15_min) / COUNT(*) AS within_15_pct,
       COUNT(*) AS delivered_orders
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY store_id, store_name ORDER BY store_id;

-- Q3: delivery time and percentage within 15 minutes by store (ELT)
SELECT store_id, store_name, average_delivery_minutes, delivered_within_15_pct,
       delivered_orders
FROM mart_delivery_sla ORDER BY store_id;

-- Q4: revenue by payment mode (ETL / ELT)
SELECT payment_mode, SUM(net_revenue) AS net_revenue, COUNT(*) AS delivered_orders
FROM etl_fact_orders WHERE order_status = 'DELIVERED'
GROUP BY payment_mode ORDER BY payment_mode;
SELECT payment_mode, net_revenue, delivered_orders
FROM mart_payment_mode_revenue ORDER BY payment_mode;

-- Q4: reconciliation exceptions by reason (ETL / ELT)
SELECT reason, COUNT(*) AS exception_rows,
       SUM(ABS(COALESCE(difference, 0))) AS absolute_amount_difference
FROM etl_payment_reconciliation GROUP BY reason ORDER BY reason;
SELECT reason, COUNT(*) AS exception_rows,
       SUM(ABS(COALESCE(difference, 0))) AS absolute_amount_difference
FROM mart_payment_reconciliation GROUP BY reason ORDER BY reason;

-- Q5: top 10 by delivered spend. Export a rank label so the evidence CSV
-- does not expose linkable customer IDs; the restricted marts retain the key.
WITH ranked AS (
    SELECT f.customer_id, c.loyalty_tier, SUM(f.net_revenue) AS spend,
           COUNT(*) AS delivered_orders
    FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
    WHERE f.order_status = 'DELIVERED'
    GROUP BY f.customer_id, c.loyalty_tier
)
SELECT 'Customer ' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY spend DESC, customer_id) AS VARCHAR), 2, '0') AS customer_ref,
       loyalty_tier, spend, delivered_orders
FROM ranked ORDER BY spend DESC, customer_id LIMIT 10;
WITH ranked AS (
    SELECT customer_id, loyalty_tier, spend, delivered_orders
    FROM mart_top_customers ORDER BY spend DESC, customer_id LIMIT 10
)
SELECT 'Customer ' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY spend DESC, customer_id) AS VARCHAR), 2, '0') AS customer_ref,
       loyalty_tier, spend, delivered_orders
FROM ranked ORDER BY spend DESC, customer_id;

-- Q6: rejected rows by source and reason (ETL), for one run ID.
SELECT source_file, failure_reason, rows_rejected
FROM etl_rejections_by_file_reason
WHERE run_id = '<run_id>'
ORDER BY source_file, failure_reason;

-- Q6: rejected rows by source and reason (ELT current staging snapshot).
SELECT source_file, failure_reason, rows_rejected
FROM mart_rejections_by_file_reason
ORDER BY source_file, failure_reason;
