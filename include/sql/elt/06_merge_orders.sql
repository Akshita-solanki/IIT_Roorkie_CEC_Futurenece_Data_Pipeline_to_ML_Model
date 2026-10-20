CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT * FROM stg_orders_source WHERE FALSE;

MERGE INTO stg_orders AS target
USING stg_orders_source AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source._loaded_at >= target._loaded_at THEN UPDATE SET
    order_ts = source.order_ts,
    customer_id = source.customer_id,
    store_id = source.store_id,
    product_id = source.product_id,
    quantity = source.quantity,
    unit_price = source.unit_price,
    discount_pct = source.discount_pct,
    order_amount = source.order_amount,
    payment_mode = source.payment_mode,
    order_status = source.order_status,
    channel = source.channel,
    delivery_minutes = source.delivery_minutes,
    dq_status = source.dq_status,
    failure_reason = source.failure_reason,
    _loaded_at = source._loaded_at,
    _source_file = source._source_file,
    _run_id = source._run_id
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct,
    order_amount, payment_mode, order_status, channel, delivery_minutes, dq_status, failure_reason,
    _loaded_at, _source_file, _run_id
) VALUES (
    source.order_id, source.order_ts, source.customer_id, source.store_id, source.product_id,
    source.quantity, source.unit_price, source.discount_pct, source.order_amount, source.payment_mode,
    source.order_status, source.channel, source.delivery_minutes, source.dq_status, source.failure_reason,
    source._loaded_at, source._source_file, source._run_id
);
