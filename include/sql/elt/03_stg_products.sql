CREATE OR REPLACE TABLE stg_products AS
WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER () AS _raw_row FROM raw_products
), ranked AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY COALESCE(NULLIF(TRIM(product_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)), _run_id, _source_file) AS _duplicate_count,
           ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(TRIM(product_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)) ORDER BY _loaded_at DESC, _raw_row DESC) AS _rank
    FROM numbered
), latest AS (SELECT * FROM ranked WHERE _rank = 1), casted AS (
    SELECT NULLIF(TRIM(product_id), '') AS product_id,
           NULLIF(TRIM(product_name), '') AS product_name,
           NULLIF(TRIM(category), '') AS category,
           NULLIF(TRIM(sub_category), '') AS sub_category,
           NULLIF(TRIM(brand), '') AS brand,
           TRY_CAST(NULLIF(TRIM(mrp), '') AS DOUBLE) AS mrp,
           TRY_CAST(NULLIF(TRIM(cost_price), '') AS DOUBLE) AS cost_price,
           NULLIF(TRIM(unit), '') AS unit,
           CASE LOWER(TRIM(is_perishable)) WHEN 'true' THEN TRUE WHEN '1' THEN TRUE WHEN 'false' THEN FALSE WHEN '0' THEN FALSE ELSE NULL END AS is_perishable,
           TRY_CAST(NULLIF(TRIM(launch_date), '') AS DATE) AS launch_date,
           _duplicate_count, _loaded_at, _source_file, _run_id
    FROM latest
), checked AS (
    SELECT *,
           (product_id IS NULL OR category IS NULL OR mrp IS NULL OR mrp <= 0 OR cost_price IS NULL OR cost_price <= 0 OR cost_price > mrp OR launch_date IS NULL OR launch_date > CURRENT_DATE) AS _critical_failure
    FROM casted
)
SELECT product_id, product_name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date,
       CASE WHEN _critical_failure THEN 'FAIL' WHEN _duplicate_count > 1 THEN 'WARN' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN _duplicate_count > 1 THEN 'duplicate product_id' END,
           CASE WHEN product_id IS NULL THEN 'missing product_id' END,
           CASE WHEN category IS NULL THEN 'missing category' END,
           CASE WHEN mrp IS NULL OR mrp <= 0 THEN 'mrp must be positive' END,
           CASE WHEN cost_price IS NULL OR cost_price <= 0 THEN 'cost_price must be positive' END,
           CASE WHEN cost_price > mrp THEN 'cost_price exceeds mrp' END,
           CASE WHEN launch_date IS NULL THEN 'invalid launch_date' END,
           CASE WHEN launch_date > CURRENT_DATE THEN 'future launch_date' END
       ), '') AS failure_reason,
       _loaded_at, _source_file, _run_id
FROM checked;
