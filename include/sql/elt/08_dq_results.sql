CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
    severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT, status VARCHAR,
    checked_at TIMESTAMP
);

DELETE FROM dq_results
WHERE run_id='{{run_id}}' AND dag_id='nammamart_elt_akshita';

INSERT INTO dq_results
WITH checks AS (
    SELECT 'orders_required_fields' AS check_name, 'completeness' AS dimension, 'Critical' AS severity,
           COUNT(*) AS rows_checked, COUNT(*) FILTER (WHERE order_id IS NULL OR order_ts IS NULL OR customer_id IS NULL OR store_id IS NULL OR product_id IS NULL OR quantity IS NULL OR unit_price IS NULL OR discount_pct IS NULL OR order_amount IS NULL OR payment_mode IS NULL OR order_status IS NULL OR (order_status='DELIVERED' AND delivery_minutes IS NULL)) AS rows_failed
    FROM stg_orders
    UNION ALL SELECT 'orders_id_unique','uniqueness','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%duplicate order_id%') FROM stg_orders
    UNION ALL SELECT 'orders_values_valid','validity','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%invalid order_ts%' OR failure_reason LIKE '%invalid payment_mode%' OR failure_reason LIKE '%invalid order_status%' OR failure_reason LIKE '%discount_pct outside%') FROM stg_orders
    UNION ALL SELECT 'orders_amount_and_delivery_accurate','accuracy','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%quantity must be positive%' OR failure_reason LIKE '%order amount formula mismatch%' OR failure_reason LIKE '%delivery_minutes must be below%') FROM stg_orders
    UNION ALL SELECT 'orders_master_keys_consistent','consistency','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%unknown or invalid%') FROM stg_orders
    UNION ALL SELECT 'orders_future_timestamp','timeliness','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%future order_ts%') FROM stg_orders
    UNION ALL SELECT 'orders_freshness_within_one_day','timeliness','Warning',1,CASE WHEN COALESCE(MAX(order_ts) FILTER (WHERE order_ts <= CURRENT_TIMESTAMP),TIMESTAMP '1970-01-01') < CURRENT_TIMESTAMP - INTERVAL '1 day' THEN 1 ELSE 0 END FROM stg_orders
    UNION ALL SELECT 'orders_volume_min_500','completeness','Warning',COUNT(*),CASE WHEN COUNT(*) < 500 THEN 1 ELSE 0 END FROM raw_orders WHERE _run_id='{{run_id}}' AND _source_file='orders.csv'
    UNION ALL SELECT 'orders_unit_price_distribution','accuracy','Warning',COUNT(*),COUNT(*) FILTER (WHERE dq_status='WARN' AND failure_reason LIKE '%unit_price exceeds%') FROM stg_orders_source
    UNION ALL SELECT 'payments_id_unique','uniqueness','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%duplicate payment_id%') FROM stg_payments
    UNION ALL SELECT 'payments_values_valid','validity','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%invalid payment_ts%' OR failure_reason LIKE '%invalid payment_mode%' OR failure_reason LIKE '%invalid payment_status%') FROM stg_payments
    UNION ALL SELECT 'payments_amount_accurate','accuracy','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%amount_paid must be non-negative%') FROM stg_payments
    UNION ALL SELECT 'payments_orders_consistent','consistency','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%unknown order_id%' OR failure_reason LIKE '%amount mismatch%') FROM stg_payments
    UNION ALL SELECT 'customers_required_fields','completeness', 'Critical',COUNT(*),COUNT(*) FILTER (WHERE customer_id IS NULL OR loyalty_tier IS NULL) FROM stg_customers
    UNION ALL SELECT 'customers_id_unique','uniqueness','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%duplicate customer_id%') FROM stg_customers
    UNION ALL SELECT 'customers_email_phone_valid','validity','Warning',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%email%' OR failure_reason LIKE '%phone%') FROM stg_customers
    UNION ALL SELECT 'customers_pincode_valid','validity','Warning',COUNT(*),COUNT(*) FILTER (WHERE pincode IS NULL OR pincode < 100000 OR pincode > 999999) FROM stg_customers
    UNION ALL SELECT 'customers_tier_consistent','consistency','Critical',COUNT(*),COUNT(*) FILTER (WHERE loyalty_tier NOT IN ('Bronze','Silver','Gold','Platinum')) FROM stg_customers
    UNION ALL SELECT 'customers_signup_timely','timeliness','Critical',COUNT(*),COUNT(*) FILTER (WHERE signup_date > CURRENT_DATE OR signup_date IS NULL) FROM stg_customers
    UNION ALL SELECT 'products_required_fields','completeness','Critical',COUNT(*),COUNT(*) FILTER (WHERE product_id IS NULL OR category IS NULL) FROM stg_products
    UNION ALL SELECT 'products_id_unique','uniqueness','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%duplicate product_id%') FROM stg_products
    UNION ALL SELECT 'products_values_valid','validity','Critical',COUNT(*),COUNT(*) FILTER (WHERE launch_date IS NULL) FROM stg_products
    UNION ALL SELECT 'products_prices_accurate','accuracy','Critical',COUNT(*),COUNT(*) FILTER (WHERE mrp <= 0 OR cost_price <= 0 OR cost_price > mrp) FROM stg_products
    UNION ALL SELECT 'products_launch_timely','timeliness','Critical',COUNT(*),COUNT(*) FILTER (WHERE launch_date > CURRENT_DATE) FROM stg_products
    UNION ALL SELECT 'stores_required_fields','completeness','Critical',COUNT(*),COUNT(*) FILTER (WHERE store_id IS NULL OR zone IS NULL) FROM stg_stores
    UNION ALL SELECT 'stores_id_unique','uniqueness','Critical',COUNT(*),COUNT(*) FILTER (WHERE failure_reason LIKE '%duplicate store_id%') FROM stg_stores
    UNION ALL SELECT 'stores_capacity_accurate','accuracy','Critical',COUNT(*),COUNT(*) FILTER (WHERE capacity_orders_per_day <= 0) FROM stg_stores
    UNION ALL SELECT 'stores_open_date_valid','validity','Critical',COUNT(*),COUNT(*) FILTER (WHERE open_date IS NULL) FROM stg_stores
    UNION ALL SELECT 'stores_open_date_timely','timeliness','Critical',COUNT(*),COUNT(*) FILTER (WHERE open_date > CURRENT_DATE) FROM stg_stores
)
SELECT '{{run_id}}', 'nammamart_elt_akshita', check_name, dimension, severity,
       rows_checked, rows_failed,
       CASE WHEN rows_failed = 0 THEN 'PASS' WHEN severity='Warning' THEN 'WARN' ELSE 'FAIL' END,
       CURRENT_TIMESTAMP
FROM checks;
