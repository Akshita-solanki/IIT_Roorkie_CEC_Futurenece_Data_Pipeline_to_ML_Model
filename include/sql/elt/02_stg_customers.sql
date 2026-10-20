CREATE OR REPLACE TABLE stg_customers AS
WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER () AS _raw_row
    FROM raw_customers
), ranked AS (
    SELECT *,
           COUNT(*) OVER (
               PARTITION BY COALESCE(NULLIF(TRIM(customer_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR)), _run_id, _source_file
           ) AS _duplicate_count,
           ROW_NUMBER() OVER (
               PARTITION BY COALESCE(NULLIF(TRIM(customer_id), ''), '__NULL__' || CAST(_raw_row AS VARCHAR))
               ORDER BY _loaded_at DESC, _raw_row DESC
           ) AS _rank
    FROM numbered
), latest AS (
    SELECT * FROM ranked WHERE _rank = 1
), casted AS (
    SELECT NULLIF(TRIM(customer_id), '') AS customer_id,
           NULLIF(TRIM(email), '') AS email_raw,
           REGEXP_REPLACE(COALESCE(phone, ''), '[^0-9]', '', 'g') AS phone_digits,
           NULLIF(TRIM(city), '') AS city,
           NULLIF(TRIM(area), '') AS area,
           TRY_CAST(NULLIF(TRIM(pincode), '') AS INTEGER) AS pincode,
           TRY_CAST(NULLIF(TRIM(signup_date), '') AS DATE) AS signup_date,
           CASE LOWER(TRIM(loyalty_tier))
               WHEN 'bronze' THEN 'Bronze' WHEN 'silver' THEN 'Silver'
               WHEN 'gold' THEN 'Gold' WHEN 'platinum' THEN 'Platinum'
               ELSE TRIM(loyalty_tier) END AS loyalty_tier,
           CASE LOWER(TRIM(is_active)) WHEN 'true' THEN TRUE WHEN '1' THEN TRUE
               WHEN 'false' THEN FALSE WHEN '0' THEN FALSE ELSE NULL END AS is_active,
           _duplicate_count, _loaded_at, _source_file, _run_id
    FROM latest
), checked AS (
    SELECT *,
           (customer_id IS NULL OR loyalty_tier NOT IN ('Bronze','Silver','Gold','Platinum') OR signup_date IS NULL OR signup_date > CURRENT_DATE) AS _critical_failure,
           (_duplicate_count > 1 OR email_raw IS NULL OR NOT REGEXP_MATCHES(email_raw, '^[^[:space:]@]+@[^[:space:]@]+[.][^[:space:]@]+$') OR LENGTH(phone_digits) <> 10 OR pincode IS NULL OR pincode < 100000 OR pincode > 999999) AS _warning_failure
    FROM casted
)
SELECT customer_id,
       CASE WHEN email_raw IS NULL THEN NULL
            WHEN REGEXP_MATCHES(email_raw, '^[^[:space:]@]+@[^[:space:]@]+[.][^[:space:]@]+$')
                THEN SUBSTR(email_raw, 1, 2) || '***@' || SPLIT_PART(email_raw, '@', 2)
            ELSE '***' END AS email_masked,
       CASE WHEN LENGTH(phone_digits) = 10 THEN SUBSTR(phone_digits,1,2) || REPEAT('*',6) || RIGHT(phone_digits,2)
            ELSE '***' END AS phone_masked,
       city, area, pincode, signup_date, loyalty_tier, is_active,
       CASE WHEN _critical_failure THEN 'FAIL' WHEN _warning_failure THEN 'WARN' ELSE 'PASS' END AS dq_status,
       NULLIF(CONCAT_WS('; ',
           CASE WHEN _duplicate_count > 1 THEN 'duplicate customer_id' END,
           CASE WHEN customer_id IS NULL THEN 'missing customer_id' END,
           CASE WHEN loyalty_tier NOT IN ('Bronze','Silver','Gold','Platinum') THEN 'invalid loyalty_tier' END,
           CASE WHEN signup_date IS NULL THEN 'invalid signup_date' END,
           CASE WHEN signup_date > CURRENT_DATE THEN 'future signup_date' END,
           CASE WHEN email_raw IS NULL THEN 'missing email' END,
           CASE WHEN email_raw IS NOT NULL AND NOT REGEXP_MATCHES(email_raw, '^[^[:space:]@]+@[^[:space:]@]+[.][^[:space:]@]+$') THEN 'invalid email format' END,
           CASE WHEN LENGTH(phone_digits) <> 10 THEN 'phone is not ten digits' END,
           CASE WHEN pincode IS NULL OR pincode < 100000 OR pincode > 999999 THEN 'pincode is not a valid six-digit Indian PIN code' END
       ), '') AS failure_reason,
       _loaded_at, _source_file, _run_id
FROM checked;
