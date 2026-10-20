CREATE OR REPLACE TABLE stg_stores AS
WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER () AS _raw_row FROM raw_stores
), ranked AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY COALESCE(NULLIF(TRIM(store_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)), _run_id, _source_file) AS _duplicate_count,
           ROW_NUMBER() OVER (PARTITION BY COALESCE(NULLIF(TRIM(store_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)) ORDER BY _loaded_at DESC, _raw_row DESC) AS _rank
    FROM numbered
), latest AS (SELECT * FROM ranked WHERE _rank = 1), casted AS (
    SELECT NULLIF(TRIM(store_id), '') AS store_id,
           NULLIF(TRIM(store_name), '') AS store_name,
           CASE LOWER(TRIM(zone)) WHEN 'north' THEN 'North' WHEN 'south' THEN 'South' WHEN 'east' THEN 'East' WHEN 'west' THEN 'West' ELSE TRIM(zone) END AS zone,
           NULLIF(TRIM(area), '') AS area,
           TRY_CAST(NULLIF(TRIM(pincode), '') AS INTEGER) AS pincode,
           TRY_CAST(NULLIF(TRIM(open_date), '') AS DATE) AS open_date,
           TRY_CAST(NULLIF(TRIM(capacity_orders_per_day), '') AS INTEGER) AS capacity_orders_per_day,
           _duplicate_count, _loaded_at, _source_file, _run_id
    FROM latest
), checked AS (
    SELECT *, (_duplicate_count > 1 OR store_id IS NULL OR zone IS NULL OR zone NOT IN ('North','South','East','West') OR open_date IS NULL OR open_date > CURRENT_DATE OR capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0) AS _critical_failure
    FROM casted
)
SELECT store_id, store_name, zone, area, pincode, open_date, capacity_orders_per_day,
       CASE WHEN _critical_failure THEN 'FAIL' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN _duplicate_count > 1 THEN 'duplicate store_id' END,
           CASE WHEN store_id IS NULL THEN 'missing store_id' END,
           CASE WHEN zone IS NULL THEN 'missing zone' END,
           CASE WHEN zone NOT IN ('North','South','East','West') THEN 'invalid zone' END,
           CASE WHEN open_date IS NULL THEN 'invalid open_date' END,
           CASE WHEN open_date > CURRENT_DATE THEN 'future open_date' END,
           CASE WHEN capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0 THEN 'capacity_orders_per_day must be positive' END
       ), '') AS failure_reason,
       _loaded_at, _source_file, _run_id
FROM checked;
