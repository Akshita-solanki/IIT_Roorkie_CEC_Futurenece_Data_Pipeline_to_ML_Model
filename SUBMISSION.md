# NammaMart Assignment Submission

> Prepared from the supplied source data and verified Airflow runs. All temporary experiment mutations were restored; the final source CSVs match their supplied versions.

## Part 0 — Discovery and data profiling

### File profiles

#### `orders.csv`

- Rows: 2,412; columns: 13.
- Nulls: `{'order_ts': 2, 'customer_id': 18, 'delivery_minutes': 164}`.
- Duplicate `order_id` rows: 24; duplicate keys: `['ORD000539', 'ORD000783', 'ORD001110', 'ORD001239', 'ORD001326', 'ORD001434', 'ORD001441', 'ORD001462', 'ORD001549', 'ORD001692', 'ORD001870', 'ORD002129']`.
- Numeric/date ranges: `order_ts: 2026-09-01 06:30:15 to 2027-01-01 09:00:00 (parse/null failures: 6); quantity: -3 to 5; unit_price: 10.0 to 883.0; discount_pct: 0 to 250; order_amount: -1593.0 to 9576.0; delivery_minutes: 8.0 to 1440.0`.
- Category counts: `{'order_status': {'DELIVERED': 2182, 'CANCELLED': 156, 'RETURNED': 69, 'Pending??': 3, 'delivered': 2}, 'payment_mode': {'UPI': 1329, 'Card': 493, 'Cash on Delivery': 326, 'Wallet': 254, 'upi': 4, 'Cashh': 2, 'UPI ': 2, 'CARD': 1, 'cod': 1}, 'channel': {'App': 2044, 'Web': 368}}`.

#### `payments.csv`

- Rows: 2,228; columns: 7.
- Nulls: `{'gateway_ref': 301}`.
- Duplicate `payment_id` rows: 8; duplicate keys: `['PAY0000509', 'PAY0000755', 'PAY0001651', 'PAY0002000']`.
- Numeric/date ranges: `payment_ts: 2026-09-01 06:30:24 to 2027-01-01 09:01:46 (parse/null failures: 0); amount_paid: -1593.0 to 9576.0`.
- Category counts: `{'payment_mode': {'UPI': 1238, 'Card': 445, 'Cash on Delivery': 301, 'Wallet': 234, 'upi': 4, 'UPI ': 2, 'Cashh': 2, 'cod': 1, 'CARD': 1}, 'payment_status': {'SUCCESS': 2116, 'REFUNDED': 68, 'FAILED': 44}}`.

#### `customers.csv`

- Rows: 405; columns: 10.
- Nulls: `{'email': 4, 'city': 6}`.
- Duplicate `customer_id` rows: 10; duplicate keys: `['C00036', 'C00103', 'C00226', 'C00252', 'C00313']`.
- Numeric/date ranges: `phone: 9804341 to 9999393305; five pincodes fail the valid six-digit Indian PIN format (three 5600XX values and two 000000 values); signup_date: 2024-06-01 00:00:00 to 2027-01-05 00:00:00 (parse/null failures: 0)`.
- Category counts: `{'loyalty_tier': {'Bronze': 185, 'Silver': 133, 'Gold': 54, 'Platinum': 25, 'BRONZE': 3, 'bronze': 3, 'SILVER': 2}}`.

#### `products.csv`

- Rows: 152; columns: 10.
- Nulls: `{'category': 3}`.
- Duplicate `product_id` rows: 4; duplicate keys: `['P0030', 'P0140']`.
- Numeric/date ranges: `mrp: -203.0 to 883.0; cost_price: 6.79 to 631.79; is_perishable: False to True; launch_date: 2023-01-03 00:00:00 to 2025-09-20 00:00:00 (parse/null failures: 0)`.
- Category counts: `{'category': {'Dairy & Bakery': 26, 'Household': 26, 'Fruits & Vegetables': 25, 'Snacks & Beverages': 25, 'Personal Care': 25, 'Staples': 22, '<NULL>': 3}}`.

#### `stores.csv`

- Rows: 15; columns: 8.
- Nulls: `{'zone': 1}`.
- Duplicate `store_id` rows: 0; duplicate keys: `none`.
- Numeric/date ranges: `pincode: 560003 to 560102; open_date: 2024-02-14 00:00:00 to 2025-07-28 00:00:00 (parse/null failures: 0); capacity_orders_per_day: 0 to 600`.
- Category counts: `{'zone': {'East': 4, 'West': 4, 'North': 3, 'South': 3, '<NULL>': 1}}`.

#### `orders_2026-10-01_increment.csv`

- Rows: 160; columns: 13.
- Nulls: `none`.
- Duplicate `order_id` rows: 0; duplicate keys: `none`.
- Numeric/date ranges: `order_ts: 2026-09-01 09:38:13 to 2026-10-01 22:33:00 (parse/null failures: 0); quantity: 1 to 4; unit_price: 10.0 to 883.0; discount_pct: 0 to 20; order_amount: 11.0 to 2052.0; delivery_minutes: 8 to 34`.
- Category counts: `{'order_status': {'DELIVERED': 150, 'RETURNED': 10}, 'payment_mode': {'UPI': 46, 'Card': 44, 'Cash on Delivery': 40, 'Wallet': 30}, 'channel': {'App': 160}}`.

### Relationships and business rules

- Orders → customers: 18 orders have null customer IDs; the remaining non-null customer keys resolve. Orders → products: 9 unknown product keys. Orders → stores: 5 unknown store keys. Payments → orders: 8 unknown order keys.
- Recomputed `order_amount` differs by more than ₹0.01 for 18 September orders. There are 10 non-positive quantities, 3 out-of-range discounts, 8 delivered orders missing delivery time, and 6 deliveries >=120 minutes.
- There are 3 invalid order statuses, 2 invalid payment mode values after normalizing case/spacing/`cod` aliases (the other variants normalize), 14 successful payment amount mismatches, and the listed duplicate primary keys. Increment file contains 160 rows: 150 new order IDs and 10 corrections.

### Data Issues Log

Each row below names one affected source row and one failed check. Duplicate-key issues are logged for every row participating in a duplicate key.

| File | Row key | Column | Problem | Quality dimension | Proposed rule | Severity |
|---|---|---|---|---|---|---|
| customers.csv | C00031 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00032 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00036 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00036 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00038 | signup_date | signup date is in the future | Timeliness | Reject future signup dates | Critical |
| customers.csv | C00045 | email | email is missing | Completeness | Keep customer but warn; mask safely as unavailable | Warning |
| customers.csv | C00062 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00063 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00067 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00092 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00095 | phone | phone is not ten digits after punctuation removal | Validity | Warn on phone values that do not normalize to 10 digits | Warning |
| customers.csv | C00103 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00103 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00112 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00130 | phone | phone is not ten digits after punctuation removal | Validity | Warn on phone values that do not normalize to 10 digits | Warning |
| customers.csv | C00133 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00136 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00142 | city | city is missing | Completeness | Warn; city is not required for order revenue | Warning |
| customers.csv | C00151 | phone | phone is not ten digits after punctuation removal | Validity | Warn on phone values that do not normalize to 10 digits | Warning |
| customers.csv | C00171 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00193 | email | email is missing | Completeness | Keep customer but warn; mask safely as unavailable | Warning |
| customers.csv | C00196 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00199 | loyalty_tier | loyalty tier casing is inconsistent | Consistency | Normalize tier casing in staging and report a warning | Warning |
| customers.csv | C00219 | city | city is missing | Completeness | Warn; city is not required for order revenue | Warning |
| customers.csv | C00226 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00226 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00233 | city | city is missing | Completeness | Warn; city is not required for order revenue | Warning |
| customers.csv | C00240 | phone | phone is not ten digits after punctuation removal | Validity | Warn on phone values that do not normalize to 10 digits | Warning |
| customers.csv | C00252 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00252 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00290 | phone | phone is not ten digits after punctuation removal | Validity | Warn on phone values that do not normalize to 10 digits | Warning |
| customers.csv | C00307 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00310 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00313 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00313 | customer_id | customer_id appears more than once | Uniqueness | Choose a deterministic master record and log duplicate keys | Critical |
| customers.csv | C00327 | email | email is missing | Completeness | Keep customer but warn; mask safely as unavailable | Warning |
| customers.csv | C00328 | city | city is missing | Completeness | Warn; city is not required for order revenue | Warning |
| customers.csv | C00337 | email | email is missing | Completeness | Keep customer but warn; mask safely as unavailable | Warning |
| customers.csv | C00339 | phone | phone is not ten digits after punctuation removal | Validity | Warn on phone values that do not normalize to 10 digits | Warning |
| customers.csv | C00355 | city | city is missing | Completeness | Warn; city is not required for order revenue | Warning |
| customers.csv | C00357 | phone | phone is not ten digits after punctuation removal | Validity | Warn on phone values that do not normalize to 10 digits | Warning |
| customers.csv | C00363 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00392 | email | email format is invalid | Validity | Warn on invalid email format; do not expose raw email in reporting | Warning |
| customers.csv | C00394 | city | city is missing | Completeness | Warn; city is not required for order revenue | Warning |
| customers.csv | C00398 | signup_date | signup date is in the future | Timeliness | Reject future signup dates | Critical |
| customers.csv | C00400 | signup_date | signup date is in the future | Timeliness | Reject future signup dates | Critical |
| customers.csv | C00083 | pincode | pincode is not six digits | Validity | Retain the customer with a warning; restrict location data in reporting | Warning |
| customers.csv | C00342 | pincode | pincode is not a valid Indian PIN code | Validity | Retain the customer with a warning; restrict location data in reporting | Warning |
| customers.csv | C00132 | pincode | pincode is not six digits | Validity | Retain the customer with a warning; restrict location data in reporting | Warning |
| customers.csv | C00166 | pincode | pincode is not six digits | Validity | Retain the customer with a warning; restrict location data in reporting | Warning |
| customers.csv | C00395 | pincode | pincode is not a valid Indian PIN code | Validity | Retain the customer with a warning; restrict location data in reporting | Warning |
| orders.csv | ORD000020 | delivery_minutes | delivery time is at or above 120 minutes | Accuracy | Quarantine delivery values >= 120 minutes | Critical |
| orders.csv | ORD000022 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD000070 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD000099 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD000134 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD000326 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD000362 | order_ts | required value is null | Completeness | Require non-null order_ts | Critical |
| orders.csv | ORD000403 | order_ts | required value is null | Completeness | Require non-null order_ts | Critical |
| orders.csv | ORD000424 | delivery_minutes | delivery time is at or above 120 minutes | Accuracy | Quarantine delivery values >= 120 minutes | Critical |
| orders.csv | ORD000455 | delivery_minutes | delivery time is at or above 120 minutes | Accuracy | Quarantine delivery values >= 120 minutes | Critical |
| orders.csv | ORD000462 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD000469 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD000500 | order_status | status is outside the allowed values | Validity | Allow DELIVERED, CANCELLED, RETURNED after trimming/case normalization | Critical |
| orders.csv | ORD000534 | order_ts | timestamp does not parse | Validity | Require an ISO-compatible timestamp | Critical |
| orders.csv | ORD000539 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD000539 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD000580 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD000583 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD000596 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD000677 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD000701 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD000731 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD000764 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD000783 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD000783 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD000789 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD000790 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD000801 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD000845 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD000867 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD000878 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD000880 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD000946 | order_ts | timestamp does not parse | Validity | Require an ISO-compatible timestamp | Critical |
| orders.csv | ORD000964 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD000965 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD000986 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD000997 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD001037 | delivery_minutes | delivery time is at or above 120 minutes | Accuracy | Quarantine delivery values >= 120 minutes | Critical |
| orders.csv | ORD001046 | store_id | store_id does not exist in stores.csv | Consistency | Require store_id to exist in the corresponding master | Critical |
| orders.csv | ORD001058 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD001089 | order_ts | timestamp is in the future relative to the profiling date 2026-10-19 | Timeliness | Reject future order timestamps; compare with the run clock | Critical |
| orders.csv | ORD001104 | delivery_minutes | delivery time is at or above 120 minutes | Accuracy | Quarantine delivery values >= 120 minutes | Critical |
| orders.csv | ORD001110 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001110 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001121 | order_ts | timestamp is in the future relative to the profiling date 2026-10-19 | Timeliness | Reject future order timestamps; compare with the run clock | Critical |
| orders.csv | ORD001124 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD001237 | discount_pct | discount is outside 0–100 percent | Validity | Require 0 <= discount_pct <= 100 | Critical |
| orders.csv | ORD001237 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD001239 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001239 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001274 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD001275 | store_id | store_id does not exist in stores.csv | Consistency | Require store_id to exist in the corresponding master | Critical |
| orders.csv | ORD001277 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD001296 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD001326 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001326 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001332 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD001373 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD001384 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD001414 | payment_mode | payment mode is outside the allowed values | Validity | Normalize UPI/card/COD aliases, then allow UPI, Card, Cash on Delivery, Wallet | Critical |
| orders.csv | ORD001434 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001434 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001441 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001441 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001459 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD001462 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001462 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001468 | discount_pct | discount is outside 0–100 percent | Validity | Require 0 <= discount_pct <= 100 | Critical |
| orders.csv | ORD001468 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD001492 | store_id | store_id does not exist in stores.csv | Consistency | Require store_id to exist in the corresponding master | Critical |
| orders.csv | ORD001515 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD001532 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD001549 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001549 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001554 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD001610 | order_ts | timestamp does not parse | Validity | Require an ISO-compatible timestamp | Critical |
| orders.csv | ORD001624 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD001637 | order_status | status is outside the allowed values | Validity | Allow DELIVERED, CANCELLED, RETURNED after trimming/case normalization | Critical |
| orders.csv | ORD001685 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD001692 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001692 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001693 | order_ts | timestamp is in the future relative to the profiling date 2026-10-19 | Timeliness | Reject future order timestamps; compare with the run clock | Critical |
| orders.csv | ORD001724 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD001725 | order_ts | timestamp is in the future relative to the profiling date 2026-10-19 | Timeliness | Reject future order timestamps; compare with the run clock | Critical |
| orders.csv | ORD001755 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD001765 | order_ts | timestamp is in the future relative to the profiling date 2026-10-19 | Timeliness | Reject future order timestamps; compare with the run clock | Critical |
| orders.csv | ORD001799 | store_id | store_id does not exist in stores.csv | Consistency | Require store_id to exist in the corresponding master | Critical |
| orders.csv | ORD001800 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD001812 | payment_mode | payment mode is outside the allowed values | Validity | Normalize UPI/card/COD aliases, then allow UPI, Card, Cash on Delivery, Wallet | Critical |
| orders.csv | ORD001870 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001870 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD001918 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD001965 | order_ts | timestamp is in the future relative to the profiling date 2026-10-19 | Timeliness | Reject future order timestamps; compare with the run clock | Critical |
| orders.csv | ORD001970 | order_status | status is outside the allowed values | Validity | Allow DELIVERED, CANCELLED, RETURNED after trimming/case normalization | Critical |
| orders.csv | ORD002004 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002012 | order_ts | timestamp does not parse | Validity | Require an ISO-compatible timestamp | Critical |
| orders.csv | ORD002019 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD002031 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002101 | delivery_minutes | delivery time is at or above 120 minutes | Accuracy | Quarantine delivery values >= 120 minutes | Critical |
| orders.csv | ORD002103 | discount_pct | discount is outside 0–100 percent | Validity | Require 0 <= discount_pct <= 100 | Critical |
| orders.csv | ORD002103 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002119 | order_ts | timestamp is in the future relative to the profiling date 2026-10-19 | Timeliness | Reject future order timestamps; compare with the run clock | Critical |
| orders.csv | ORD002126 | store_id | store_id does not exist in stores.csv | Consistency | Require store_id to exist in the corresponding master | Critical |
| orders.csv | ORD002128 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002129 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD002129 | order_id | order_id appears more than once | Uniqueness | Quarantine all rows for the duplicate key until resolved | Critical |
| orders.csv | ORD002149 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD002156 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD002179 | product_id | product_id does not exist in products.csv | Consistency | Require product_id to exist in the corresponding master | Critical |
| orders.csv | ORD002219 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD002223 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002229 | delivery_minutes | delivered order has no delivery time | Completeness | Require delivery_minutes for DELIVERED orders | Critical |
| orders.csv | ORD002244 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002255 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD002262 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| orders.csv | ORD002298 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002305 | order_amount | order amount differs from quantity × unit price × (1 − discount/100) | Accuracy | Recompute and require agreement to the nearest paisa | Critical |
| orders.csv | ORD002360 | quantity | quantity is not positive | Accuracy | Require quantity > 0 | Critical |
| orders.csv | ORD002367 | customer_id | required value is null | Completeness | Require non-null customer_id | Critical |
| payments.csv | PAY0000009 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000030 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000044 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000044 | payment_mode | payment mode is outside the allowed values | Validity | Normalize and allow UPI, Card, Cash on Delivery, Wallet | Critical |
| payments.csv | PAY0000048 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000078 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000084 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000087 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000094 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000098 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000100 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000127 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000127 | payment_ts | payment timestamp is in the future relative to profiling date | Timeliness | Reject future payment timestamps | Critical |
| payments.csv | PAY0000133 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000146 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000154 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000155 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000160 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000161 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000165 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000167 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000178 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000187 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000190 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000196 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000208 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000209 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000217 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000224 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000234 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000251 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000263 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000265 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000267 | payment_ts | payment timestamp is in the future relative to profiling date | Timeliness | Reject future payment timestamps | Critical |
| payments.csv | PAY0000288 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000289 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000305 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000316 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000323 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000326 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000328 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000330 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000337 | payment_ts | payment timestamp is in the future relative to profiling date | Timeliness | Reject future payment timestamps | Critical |
| payments.csv | PAY0000349 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000354 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000367 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000380 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000382 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000385 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000386 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000389 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000396 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000396 | payment_ts | payment timestamp is in the future relative to profiling date | Timeliness | Reject future payment timestamps | Critical |
| payments.csv | PAY0000402 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000410 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000411 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000412 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000415 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000429 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000433 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000436 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000437 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000440 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000445 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000452 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000453 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000469 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000471 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000495 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000497 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000500 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000504 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000505 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000509 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0000509 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0000518 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000520 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000522 | payment_mode | payment mode is outside the allowed values | Validity | Normalize and allow UPI, Card, Cash on Delivery, Wallet | Critical |
| payments.csv | PAY0000531 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000538 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000542 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000544 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000547 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000557 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000570 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000577 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000586 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000588 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000591 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000602 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000605 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000610 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000614 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000617 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000622 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000624 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000626 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000642 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000654 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000659 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000668 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000679 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000681 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000696 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000701 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000713 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000721 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000725 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000737 | amount_paid | payment amount is negative | Accuracy | Require non-negative payment amount | Critical |
| payments.csv | PAY0000741 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000744 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000747 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000751 | amount_paid | payment amount is negative | Accuracy | Require non-negative payment amount | Critical |
| payments.csv | PAY0000753 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000755 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0000755 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0000762 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000779 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000789 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000791 | amount_paid | payment amount is negative | Accuracy | Require non-negative payment amount | Critical |
| payments.csv | PAY0000796 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000797 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000810 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000812 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000833 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000842 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000848 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000863 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000865 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000870 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000881 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000882 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000883 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000885 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000902 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000909 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0000920 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000935 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000944 | amount_paid | payment amount is negative | Accuracy | Require non-negative payment amount | Critical |
| payments.csv | PAY0000949 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000959 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000977 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000990 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000995 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0000998 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001004 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001025 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001032 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001037 | payment_ts | payment timestamp is in the future relative to profiling date | Timeliness | Reject future payment timestamps | Critical |
| payments.csv | PAY0001043 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001045 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001053 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001060 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001063 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001066 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001072 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001080 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001097 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001098 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001118 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001131 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001135 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001139 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0001141 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001147 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001149 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001150 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001156 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001167 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0001167 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001178 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001185 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001186 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001202 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001210 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001214 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001217 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001229 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001240 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001251 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001256 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001260 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001268 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001274 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001292 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001311 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001315 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001317 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001318 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001320 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001334 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001335 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001349 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001352 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001360 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001367 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001368 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001379 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001391 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001397 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001404 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001409 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001415 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001416 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001431 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001432 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001435 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001437 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001449 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001450 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001451 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001460 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001463 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001477 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001488 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001488 | payment_ts | payment timestamp is in the future relative to profiling date | Timeliness | Reject future payment timestamps | Critical |
| payments.csv | PAY0001492 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001494 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001497 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001498 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001499 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0001511 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001514 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001522 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001524 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001527 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001542 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001557 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001560 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001561 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001568 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0001568 | amount_paid | payment amount is negative | Accuracy | Require non-negative payment amount | Critical |
| payments.csv | PAY0001579 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001580 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001583 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001585 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001592 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001596 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001608 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001617 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001627 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001628 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001630 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001632 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001633 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001634 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001650 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001651 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0001651 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0001684 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001685 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001691 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001719 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001737 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001744 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001745 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001753 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001754 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001755 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001761 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001771 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001772 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001778 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001780 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001799 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001813 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0001817 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001821 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001825 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001828 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001842 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001846 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001852 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001855 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001860 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001866 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001869 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001873 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001877 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001888 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001890 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001902 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001903 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001908 | payment_ts | payment timestamp is in the future relative to profiling date | Timeliness | Reject future payment timestamps | Critical |
| payments.csv | PAY0001918 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001928 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001938 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001948 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001951 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001963 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001983 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001984 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001991 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001994 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0001998 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002000 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0002000 | payment_id | payment_id appears more than once | Uniqueness | Quarantine duplicate payment keys pending gateway reconciliation | Critical |
| payments.csv | PAY0002003 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002008 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002011 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002017 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002023 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002024 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002028 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002029 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002046 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002049 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002059 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002062 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002079 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002082 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002100 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002106 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002114 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002122 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002127 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002130 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002132 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002136 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002137 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002143 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002145 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002152 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002153 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002154 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002155 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002162 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002172 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002175 | amount_paid | payment amount is negative | Accuracy | Require non-negative payment amount | Critical |
| payments.csv | PAY0002179 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002181 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002183 | amount_paid | SUCCESS payment amount differs from associated order amount | Consistency | Require successful payment to equal the canonical order amount; retain mismatch in reconciliation | Critical |
| payments.csv | PAY0002189 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002190 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002195 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002196 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002203 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002209 | gateway_ref | gateway reference is missing | Completeness | Warn when gateway_ref is unavailable; retain payment for reconciliation | Warning |
| payments.csv | PAY0002217 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| payments.csv | PAY0002218 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| payments.csv | PAY0002219 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| payments.csv | PAY0002220 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| payments.csv | PAY0002221 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| payments.csv | PAY0002222 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| payments.csv | PAY0002223 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| payments.csv | PAY0002224 | order_id | payment references an order absent from orders.csv | Consistency | Require payment order_id to resolve to an order | Critical |
| products.csv | P0019 | cost_price | cost price exceeds MRP | Accuracy | Require cost_price <= mrp | Critical |
| products.csv | P0030 | product_id | product_id appears more than once | Uniqueness | Choose a deterministic product record and log duplicate keys | Critical |
| products.csv | P0030 | product_id | product_id appears more than once | Uniqueness | Choose a deterministic product record and log duplicate keys | Critical |
| products.csv | P0051 | category | category is missing | Completeness | Require category for category margin reporting | Critical |
| products.csv | P0060 | category | category is missing | Completeness | Require category for category margin reporting | Critical |
| products.csv | P0068 | category | category is missing | Completeness | Require category for category margin reporting | Critical |
| products.csv | P0073 | cost_price | cost price exceeds MRP | Accuracy | Require cost_price <= mrp | Critical |
| products.csv | P0074 | cost_price | cost price exceeds MRP | Accuracy | Require cost_price <= mrp | Critical |
| products.csv | P0109 | cost_price | cost price exceeds MRP | Accuracy | Require cost_price <= mrp | Critical |
| products.csv | P0109 | mrp | MRP is not positive | Accuracy | Require mrp > 0 | Critical |
| products.csv | P0117 | cost_price | cost price exceeds MRP | Accuracy | Require cost_price <= mrp | Critical |
| products.csv | P0140 | product_id | product_id appears more than once | Uniqueness | Choose a deterministic product record and log duplicate keys | Critical |
| products.csv | P0140 | product_id | product_id appears more than once | Uniqueness | Choose a deterministic product record and log duplicate keys | Critical |
| stores.csv | S07 | zone | zone is missing | Completeness | Require a zone for zone-level reporting | Critical |
| stores.csv | S12 | capacity_orders_per_day | daily order capacity is not positive | Accuracy | Require capacity_orders_per_day > 0 | Critical |

**Logged issue count:** 531 row/check findings across the six files.

### Questions for the client

1. Should a negative quantity ever represent a return, or should every negative-quantity order be rejected as an entry error?
2. During a historical replay, should a dataset whose newest source timestamp is more than one day old block finance reporting, or warn while preserving the replay?
3. When a next-day correction changes DELIVERED to RETURNED, should the default report restate September current revenue, or preserve both as-was and current-state views?

## Parts A–G

### Part A — ETL implementation and observed run

The ETL DAG extracts orders, payments, customers, products, and stores in parallel; validates them in Python; writes failed source rows to run-specific quarantine CSVs; masks customer email and phone in the reporting star schema; and writes quality, rejection summaries, reconciliation, report, and run-audit outputs to DuckDB. The incremental file is merged by `order_id`. The full fact snapshot is replaced on every run, so a rerun does not add duplicate fact rows.

Final manually verified ETL run used for the tabular evidence: `manual__2026-10-19T19:01:09.921049+00:00` (`SUCCESS`, 2026-10-20 00:31 IST). It extracted 5,372 source rows, loaded 2,013 fact rows, and quarantined 1,070 source rows across all five input files. Of the fact rows, 1,827 are DELIVERED; September net revenue is ₹985,561.65. The run wrote 64 quality checks (31 critical checks with failures and 7 warning checks with warnings; rows can fail multiple checks). The pincode validity warning found five invalid customer pincodes. The run-audit record is in `pipeline_runs`; row/reason summaries are in `etl_rejections_by_file_reason`; the masked dimensions and facts are `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`, and `etl_fact_orders`.

### Part B — ELT implementation and observed run

The ELT DAG appends the CSV values as text to five `raw_*` tables with source, run, and load metadata. SQL files then build typed, quality-scored staging tables, merge the increment by `order_id`, and build the reporting marts. The final manually verified ELT run used for the tabular evidence is `manual__2026-10-19T19:01:18.024800+00:00` (2026-10-20 00:31 IST); it extracted 5,372 source rows and produced 2,013 non-failed current order rows. The current-state staging snapshot used for these exports includes 542 failed orders: 537 from the supplied files plus five rows retained from the five-row observability experiment in append-only raw history. At the time of this run, the raw-orders table contained 49,083 records because it appends each run's base and increment inputs; raw growth on rerun is intentional for audit history.

The passing order population and September mart totals match ETL: 1,827 DELIVERED orders and ₹985,561.65 net revenue. Category revenue and gross margin, delivery SLA, payment mode, top-customer rankings, and clean-order reconciliation match. Reconciliation evaluates quality-passing orders; all raw order IDs are used only to distinguish rejected known orders from truly unknown payment references. The warehouse files are local runtime artifacts and are excluded from version control because ELT raw customer records contain personal data.

### Part C — Schedule

- Cron: `0 9 * * 1-6` (09:00 Monday through Saturday).
- Time zone: `Asia/Kolkata`; 09:00 IST is 03:30 UTC. The successful scheduled DAG run shown by Airflow had logical time `2026-10-19T03:30:00+00:00`.
- If the cron is evaluated using a UTC `start_date`, 09:00 UTC is 14:30 IST, too late for the leadership review.
- `catchup=False` avoids automatically replaying every missed daily interval after a local laptop restart. If the laptop or Docker is off at 09:00, this local deployment does not run; the missed interval is not backfilled. A production SLA needs an always-on Airflow host and alerting.
- Both DAGs use two retries, a two-minute retry delay, a 30-minute task execution timeout, `catchup=False`, and `max_active_runs=1`. The last setting serializes runs against each single-writer DuckDB file and prevents the conflicting file locks encountered during overlapping local runs.

### Part D — Data quality dimensions

Both pipelines check completeness, uniqueness, validity, accuracy, consistency, and timeliness. ETL computes critical/warning checks in `include/nammamart_quality.py`, quarantines critical failures, and persists every check to `dq_results`. ELT applies SQL rules in staging and writes the same scorecard shape to `dq_results`. The manually verified ETL and ELT runs referenced in Parts A and B wrote 64 and 29 scorecard rows, respectively; all six dimensions are represented. The ETL scorecard has 31 failing critical checks and 7 warnings (1,466 findings); ELT has 17 failing critical checks and 3 warnings (667 findings). Critical row failures remain out of reporting marts; warning rows remain available with `dq_status='WARN'` and a reason.

The checks include required order fields and delivery time, duplicate IDs, enumerated statuses and payment modes, timestamp parsing, amount formula, positive quantity and prices, delivery-time bounds, foreign keys to master data, payment/order amount agreement, and future/stale timestamps. A warning volume threshold of 500 base orders and a distribution warning for prices above five times product MRP are present in the SQL scorecard and ETL validation.

### Business questions — SQL and results

The complete, runnable query pairs are in [docs/business-queries.sql](docs/business-queries.sql). CSV output from both live DuckDB warehouses is saved under [docs/evidence/query-results](docs/evidence/query-results). Question 5 extracts use rank labels and loyalty tiers; they contain no customer IDs, email, phone, or name.

1. **Daily revenue by zone/store:** 397 grouped date/store rows in each warehouse. Zone totals across September are East ₹290,392.65; North ₹229,060.45; South ₹238,118.75; West ₹227,989.80; total ₹985,561.65. Each ETL and ELT daily row matches.
2. **Category revenue and gross margin:**

   | Category | Revenue | Gross margin |
   | --- | ---: | ---: |
   | Dairy & Bakery | ₹156,279.05 | ₹41,356.24 |
   | Fruits & Vegetables | ₹80,820.30 | ₹19,084.54 |
   | Household | ₹177,012.30 | ₹45,151.70 |
   | Personal Care | ₹187,235.40 | ₹41,961.91 |
   | Snacks & Beverages | ₹159,733.05 | ₹37,235.14 |
   | Staples | ₹224,481.55 | ₹57,334.78 |

3. **Delivery SLA by store:** both outputs include 13 stores with the mean delivery minutes and percent within 15 minutes. Overall mean is about 21.4 minutes; Whitefield (S08) is highest for the 15-minute share at 35.48%, and Basaveshwaranagar (S15) is lowest at 20.98%. Exact store values are in the two Q3 CSVs.
4. **Payment mode and reconciliation:** Card ₹201,396.65 (372 orders); Cash on Delivery ₹138,615.40 (250); UPI ₹531,710.70 (995); Wallet ₹113,838.90 (210). The final clean-order baseline has 211 reconciliation exceptions: 11 amount mismatches with ₹560.00 total absolute difference, 185 delivered orders missing a successful payment, 7 successful payments for non-delivered clean orders, and 8 payments for unknown orders. ETL and ELT detail and grouped exports match.
5. **Top 10 customers:** ETL and ELT return the same ten ranked customers, loyalty tiers, delivered spend, and order counts. The reporting marts retain restricted customer keys, while the submission CSVs use rank labels (`Customer 01`–`Customer 10`) to reduce linkability; no direct contact fields are exported.
6. **Rejected rows by file/reason:** ETL exposes a run-scoped summary table `etl_rejections_by_file_reason`; ELT exposes `mart_rejections_by_file_reason`. The manually verified ETL run quarantined 1,070 source records across the input files. The corresponding ELT run audit counts 542 failed current order rows, including five synthetic rows retained from the observability experiment; the ELT Q6 output breaks out staging failures by file and reason. These run-level counts differ because ETL counts failed rows across all five inputs while the ELT audit's `rows_quarantined` field counts failed order rows. Reason findings can overlap when one row violates multiple rules.

### Part E — Governance answers

1. **Ownership and rule changes.** Ops owns order and store operational definitions; Finance owns payments, revenue, and reconciliation; CRM owns customer attributes; merchandising owns product and cost fields; the data platform owner maintains warehouse models. Finance and Ops jointly approve revenue and delivery-rule changes, with the data owner recording version, effective date, and impact before deployment.

2. **Data dictionary.** The implemented fact/dimension fields are summarized below. Types reflect the observed DuckDB outputs; source means the dataset or join from which the field is derived.

   | Table / column | Type | Meaning and source | Allowed values / PII |
   | --- | --- | --- | --- |
   | `etl_fact_orders.order_id` | VARCHAR | Order key from orders | Source ID; no direct PII |
   | `etl_fact_orders.order_ts`, `order_date` | VARCHAR | Order timestamp and derived day from orders | Timestamp / date; no |
   | `etl_fact_orders.customer_id` | VARCHAR | CRM customer key | Pseudonymous personal link; restricted |
   | `etl_fact_orders.email_masked`, `phone_masked` | VARCHAR | Masked CRM email and phone | Masked personal data; restricted |
   | `etl_fact_orders.store_id`, `store_name`, `zone` | VARCHAR | Store master attributes | Zone is North/South/East/West; no |
   | `etl_fact_orders.product_id`, `product_name`, `category` | VARCHAR | Product master attributes | Six source categories; no |
   | `etl_fact_orders.quantity` | BIGINT | Units ordered | Positive integer; no |
   | `etl_fact_orders.unit_price`, `order_amount`, `net_revenue`, `gross_margin` | DOUBLE | Source price/amount and derived delivered revenue/margin | INR; no |
   | `etl_fact_orders.discount_pct` | BIGINT | Source discount percent | 0–100; no |
   | `etl_fact_orders.payment_mode`, `order_status` | VARCHAR | Source method/status | UPI/Card/Cash on Delivery/Wallet; DELIVERED/CANCELLED/RETURNED |
   | `etl_fact_orders.delivery_minutes`, `within_15_min` | DOUBLE/BOOLEAN | Delivery SLA source and derived flag | Minutes / true-false; no |
   | `etl_dim_customer.customer_id` | VARCHAR | CRM customer key | Pseudonymous personal link; restricted |
   | `etl_dim_customer.email_masked`, `phone_masked` | VARCHAR | Masked CRM contact fields | Masked personal data; restricted |
   | `etl_dim_customer.city`, `area`, `pincode`, `loyalty_tier`, `signup_date`, `is_active` | VARCHAR | CRM attributes | Location can re-identify; restricted; tier Bronze/Silver/Gold/Platinum |
   | `etl_dim_product.product_id`, `product_name`, `category`, `sub_category`, `brand`, `unit` | VARCHAR | Product catalogue | No |
   | `etl_dim_product.mrp`, `cost_price` | DOUBLE | Catalogue prices/cost | INR; no |
   | `etl_dim_store.store_id`, `store_name`, `zone`, `area`, `pincode` | VARCHAR | Store master | No |
   | `etl_payment_reconciliation.reason` | VARCHAR | Reconciliation result from orders/payments | mismatch, missing, orphan, or non-delivered payment reason; no |
   | `dq_results` / `pipeline_runs` fields | VARCHAR/BIGINT/TIMESTAMP | Quality check and run metadata | Operational metadata; no direct PII |

   ELT staging and marts have equivalent business meanings; raw tables additionally preserve source columns as text plus `_loaded_at`, `_source_file`, and `_run_id`.

3. **Lineage.** For 1 September 2026 at Whitefield (S08), the daily mart row is ₹2,984.70 from four delivered orders. The source records are `ORD000008` (orders.csv line 1050), `ORD000051` (line 229), `ORD000056` (line 193), and `ORD000079` (line 1351). In ETL, follow `etl_daily_zone_revenue` to fact rows and then `order_id` values in `orders.csv`; the clean fact does not preserve a physical source line number. In ELT, follow `mart_daily_store_kpis` to `stg_orders`, then `_source_file` / `_run_id` into append-only `raw_orders`, where the original values and load metadata remain available. ELT is easier for exact source reconstruction; a production version should also store source row number and immutable file checksum.

4. **Personal data and applicable law.** Customer `full_name`, `email`, and `phone` are direct personal data. Customer city, area, pincode, signup date, loyalty tier, and customer ID can identify or profile a person when combined, so they receive restricted access. Store `manager_name` is also a person's name. Reporting masks email and phone and omits full name; masking supports necessary operations while reducing exposure, but is pseudonymization rather than anonymization because a person can still be linked through the customer ID. India's Digital Personal Data Protection Act, 2023 applies to digital personal data and the DPDP Rules, 2025 establish implementation duties and a phased commencement schedule. NammaMart should document purpose and notice/consent where required, minimize collection, secure processing, support data-principal rights and erase data when its purpose/retention basis ends. Confirm effective provisions and retention duties with counsel before production rollout. [Official Act (MeitY)](https://www.meity.gov.in/writereaddata/files/Digital%20Personal%20Data%20Protection%20Act%202023.pdf); [Official Rules and enforcement timeline (MeitY)](https://www.meity.gov.in/documents/act-and-policies/digital-personal-data-protection-rules-2025-gDOxUjMtQWa?pageTitle=Digit).

5. **Access control.** Ops and Finance may read approved marts. Data engineers may read masked staging and operational metadata. Raw customer data and quarantine files containing original rows should be limited to a small, audited data-protection/engineering group with a documented incident or replay purpose. The local demo uses file-level DuckDB and does not implement per-table roles, so it demonstrates masking and separation but is not production access control.

6. **Retention.** Proposed policy pending Legal/Finance approval: retain raw records only for the documented audit/legal period (initial proposal: 13 months), quarantine files for 30 days after resolution, and masked reporting facts for 24 months; then securely delete or aggregate irreversibly, subject to legal holds and statutory accounting requirements. A scheduled deletion job and deletion log must enforce the policy. These periods are a business proposal, not a conclusion that the law sets these exact durations.

7. **Secrets.** No external secret is required by the current local pipeline. Any future Slack webhook or credential belongs in an Airflow Connection/Variable or a managed secrets backend and must not be embedded in DAG source or Git. Astro logs reported the managed secrets backend was disabled in this environment; that should be enabled/configured before production secrets are used.

8. **Rejected-order audit.** The ETL quarantine file is the exact rejected source row plus `failure_reason`; its run-specific file name ties it to a run. `etl_fact_orders` contains only critical-check-passing orders, so rejected rows cannot contribute to CFO revenue. ELT retains the exact source row in `raw_orders`, with its failure reason in staging; `mart_daily_store_kpis` filters `dq_status='FAIL'`, proving the rejected row contributes nothing. The quarantine CSV and raw warehouse are restricted because exact source rows can contain customer details.

9. **Late status change.** The increment merge overwrites the current status for the ten overlapping IDs and adds new IDs, so a September order changed to RETURNED is removed from current delivered revenue after the increment. Finance usually needs this current-state restatement; audit users also need the originally reported figure. Keep both by storing immutable order-version history with `valid_from`/`valid_to` (SCD2 or an append-only event ledger) and label reports as as-reported versus current-state. The current demo upserts current state and does not yet preserve both historical states in the marts.

### Part F — Observability experiments

The green graph captures are [ETL](docs/evidence/nammamart_etl_akshita-graph.png) and [ELT](docs/evidence/nammamart_elt_akshita-graph.png). The ETL graph includes the `persist_rejections` task. The [query outputs](docs/evidence/query-results/) include the six business answers for both pipelines, run-scoped DQ and audit rows, and full payment reconciliation details. [Final run metrics](docs/evidence/final-run-metrics.json) and [the experiment record](docs/evidence/observability-experiments.json) identify the clean submission runs and all ten observed experiments.

| Experiment | Expected | Observed on ETL | Observed on ELT | Evidence | What I would change |
| --- | --- | --- | --- | --- | --- |
| 1. Happy path | Complete graph succeeds; row counts and rejected rows are auditable | Final run `manual__2026-10-19T19:01:09.921049+00:00`: 5,372 extracted, 2,013 fact rows, 1,070 quarantined | Final run `manual__2026-10-19T19:01:18.024800+00:00`: 5,372 extracted, 2,013 passing current rows, 542 rejected current orders including experiment history | Green run captures, `pipeline_runs`, DQ CSVs, and query outputs | Add failure notifications and show source-level pass/fail totals in the audit row. |
| 2. Re-run | Facts and current marts stay idempotent; ELT raw history grows | 2,013 current fact rows after repeated loads | 1,827 delivered / ₹985,561.65; raw history appends each run | Final metrics and run-scoped audit CSVs | Add an automated pre/post row-count assertion to the scheduled run. |
| 3. Increment | Add 150 new orders and apply 10 corrections without duplicate keys | Increment processed; final snapshot contains 2,013 fact rows | 160 increment rows applied; final delivered totals match ETL | Input profile and matching final marts | Persist inserted and updated counts as separate `pipeline_runs` columns. |
| 4. Missing source | Retry while source is absent; recover after source is restored | `extract_products` retried and succeeded on try 2 | `extract_sources` retried and succeeded on try 2 | Runs `exp_missing_source_etl_1792431859` and `exp_missing_source_elt_1792432014` | Alert an operator when retries are exhausted and identify the missing path in the alert. |
| 5. Five bad rows | Defects are rejected and recorded in DQ | Five injected defects were detected; run succeeded with DQ failures recorded | Same five defects were retained in raw history and rejected in staging | Runs `exp_five_bad_rows_*_1792431475`; final DQ CSVs show clean input run results | Add a run-scoped quarantine manifest with source file, reason, and evidence path. |
| 6. Schema change | Renaming required `order_amount` fails clearly | `validate_sources` failed on the missing required column | `extract_sources` failed on the missing required column; audit marked the run failed | Runs `exp_schema_change_etl_1792432192` and `exp_schema_change_elt_1792432846`; source restored afterward | Compare actual headers to a versioned schema contract and log both sets of columns. |
| 7. Volume anomaly | 50 orders warns below the 500-order floor | `orders_volume_min_500` recorded WARN | `orders_volume_min_500` recorded WARN | Runs `exp_volume_50_*_1792431577` | Make the minimum configurable by source and alert on a relative drop from recent runs. |
| 8. Distribution drift | Price distribution warning and store revenue change are visible | `orders_unit_price_distribution` recorded 167 warnings; S08 revenue moved to ₹6,932,735.90 | `orders_unit_price_distribution` recorded 159 warnings; S08 revenue moved to ₹6,932,735.90 | Runs `exp_price_drift_100x_S08_*_1792431667` | Compare price and revenue distributions with rolling store/category baselines. |
| 9. Freshness | 30-day-old maximum valid order timestamp warns | `orders_freshness_within_one_day` and increment freshness both warned | `orders_freshness_within_one_day` warned after the check was updated to ignore future timestamps | ETL run `exp_correct_stale_30_days_etl_1792434057`; ELT run `exp_correct_stale_30_days_elt_1792434213` | Track expected source business date and publish a freshness SLA/alert. |
| 10. Recovery | Clear reruns tasks after fixing the source | Schema-failure run was cleared; tasks reran successfully | Schema-failure run was cleared; tasks reran successfully after correcting a header contract false positive | Run IDs from experiment 6; all test mutations were restored | Document a recovery runbook and confirm downstream marts/audit rows after each clear. |

**Observability pillars:** Freshness is checked by timestamp rules; volume by the 500-order threshold; schema by required-column validation/SQL dependencies; distribution by unit price versus product MRP; lineage by ELT's raw file/run metadata and downstream SQL. ELT lineage is strongest. Production alerting and automated drift baselines are weakest; the local project has no external notification channel configured.

### Part G — Recommendation to Sameer, CTO

**Recommendation: adopt ELT as the primary analytical pipeline and publish finance marts only from quality-passing staging rows.** The observed run retained 49,083 raw order records across repeated loads, while its current marts matched ETL at 1,827 delivered orders and ₹985,561.65. The two approaches also agreed on all six category totals, four payment-mode totals, top-customer rankings, and the 211 clean-order reconciliation exceptions, including ₹560.00 of amount mismatches. ELT makes it easier to inspect exactly what arrived and replay transformations when the quality rules change. A critical DQ failure stays out of the current reporting marts, which protects the finance-facing number.

The ETL prototype also has a valuable guarantee: it quarantines critical failures before loading a clean fact snapshot. For finance, expose only the ELT curated mart with critical checks enforced and reconcile it to the gateway each run; retain the ETL path as a validation comparison until the production control is proven. This avoids making raw access part of the finance workflow while preserving a full audit trail for engineering and compliance.

At 1,000 times the sample volume, the local single-file DuckDB design and one Airflow worker are not a target production architecture. Move raw files to durable object storage, use partitioned columnar formats, and choose a warehouse/lakehouse engine that supports concurrent readers/writers and managed access control. Keep SQL transformations and the quality scorecard portable where practical. The assignment run demonstrates low local software cost and fast iteration; production costs would be driven by managed storage/compute, observability, backup, and retention controls rather than paid tooling in the prototype.

Top risks to manage are: (1) raw data and quarantine contain personal information and require enforced access/retention; (2) current-state upserts restate old periods and do not preserve the previously reported value; (3) a laptop-hosted schedule cannot guarantee 09:00 delivery or alert on missed runs. Before launch, add least-privilege access, immutable correction history, source freshness/volume alerts, and an always-on Airflow deployment.

### Submission evidence and final review items

- [ETL green graph](docs/evidence/nammamart_etl_akshita-graph.png) and [ELT green graph](docs/evidence/nammamart_elt_akshita-graph.png).
- [Query outputs](docs/evidence/query-results/) include six answers for each pipeline, full reconciliation rows, and grouped reconciliation totals.
- [Final run metrics](docs/evidence/final-run-metrics.json) and [observability experiment records](docs/evidence/observability-experiments.json).
- [Implementation plan](docs/nammamart-implementation-plan.md) and [design document](docs/nammamart-design.md) describe the build.
- All ten observability experiments were exercised against both pipelines. The DAGs are enabled in Airflow with schedule `0 9 * * 1-6` in Asia/Kolkata; the schedule screenshots show successful runs at 09:00 and the next scheduled time. The assignment's exact 09:00 schedule is implemented; it does not provide a before-09:00 delivery guarantee.
- The schedule screenshots show a later successful run (2026-10-20 09:00 IST) than the manually verified runs referenced by the query CSVs and quarantine task log (2026-10-20 about 00:31 IST). Both are successful; the files identify which run their figures come from. Evidence screenshots cover both DAG schedules, an ETL product quarantine CSV, DQ results, run history, and reconciliation summaries/detail; see [docs/evidence/README.md](docs/evidence/README.md).
- Do not add either local DuckDB warehouse file or run-specific quarantine CSVs to the submission repository. `.gitignore` excludes them because they hold raw customer data; retain the source dataset only in the assignment-approved `include/data/nammamart/` location.
