import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from nammamart_common import mask_email, mask_phone, normalize_text, pipeline_status_from_task_states, upsert_orders, validate_csv_headers
from nammamart_etl import build_payment_reconciliation, build_reporting_frames
from nammamart_quality import validate_frame


class NammaMartNormalizationTests(unittest.TestCase):
    def test_pipeline_status_reflects_failed_or_blocked_tasks(self):
        self.assertEqual(pipeline_status_from_task_states({"extract": "success", "load": "failed"}), "FAILED")
        self.assertEqual(pipeline_status_from_task_states({"extract": "upstream_failed"}), "FAILED")
        self.assertEqual(pipeline_status_from_task_states({"extract": "success", "audit": "running"}), "SUCCESS")

    def test_csv_header_validation_rejects_missing_required_column(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as tmp_dir:
            source = Path(tmp_dir) / "orders.csv"
            source.write_text("order_id,order_total\nO-1,12.00\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "order_amount"):
                validate_csv_headers(source, {"order_id", "order_amount"})

    def test_normalize_text_trims_and_maps_payment_aliases(self):
        self.assertEqual(normalize_text(" UPI ", field="payment_mode"), "UPI")
        self.assertEqual(normalize_text("cod", field="payment_mode"), "Cash on Delivery")
        self.assertEqual(normalize_text(" delivered ", field="order_status"), "DELIVERED")

    def test_masks_email_and_ten_digit_phone(self):
        self.assertEqual(mask_email("priya@example.com"), "pr***@example.com")
        self.assertEqual(mask_phone("9876543210"), "98******10")

    def test_incremental_upsert_updates_overlaps_and_is_idempotent(self):
        current = pd.DataFrame([
            {"order_id": "O-1", "order_status": "DELIVERED"},
            {"order_id": "O-2", "order_status": "DELIVERED"},
        ])
        increment = pd.DataFrame([
            {"order_id": "O-2", "order_status": "RETURNED"},
            {"order_id": "O-3", "order_status": "DELIVERED"},
        ])

        merged, inserted, updated = upsert_orders(current, increment)
        repeated, inserted_again, updated_again = upsert_orders(merged, increment)

        self.assertEqual((inserted, updated), (1, 1))
        self.assertEqual((inserted_again, updated_again), (0, 0))
        self.assertEqual(len(repeated), 3)
        self.assertEqual(repeated.set_index("order_id").loc["O-2", "order_status"], "RETURNED")

    def test_validation_rejects_critical_failures_and_records_six_dimensions(self):
        orders = pd.DataFrame([{
            "order_id": "O-1",
            "order_ts": "2027-01-01 09:00:00",
            "customer_id": "missing-customer",
            "store_id": "S-1",
            "product_id": "P-1",
            "quantity": -1,
            "unit_price": 10.0,
            "discount_pct": 5.0,
            "order_amount": 1.0,
            "payment_mode": "Cashh",
            "order_status": "SHIPPED",
            "delivery_minutes": None,
        }])
        references = {
            "customers": pd.DataFrame([{"customer_id": "C-1"}]),
            "products": pd.DataFrame([{"product_id": "P-1"}]),
            "stores": pd.DataFrame([{"store_id": "S-1"}]),
        }

        result = validate_frame(
            "orders.csv", orders, references, pd.Timestamp("2026-10-19T00:00:00Z")
        )

        self.assertEqual(len(result.passed), 0)
        self.assertEqual(len(result.failed), 1)
        self.assertIn("order_status", result.failed.iloc[0]["failure_reason"])
        self.assertIn("consistency", {check["dimension"] for check in result.checks})
        self.assertEqual(
            {"completeness", "uniqueness", "validity", "accuracy", "consistency", "timeliness"},
            {check["dimension"] for check in result.checks},
        )

    def test_validation_warns_on_volume_price_drift_and_stale_snapshot(self):
        orders = pd.DataFrame([{
            "order_id": "O-1", "order_ts": "2026-09-01 10:00:00", "customer_id": "C-1",
            "store_id": "S-1", "product_id": "P-1", "quantity": 1, "unit_price": 1000,
            "discount_pct": 0, "order_amount": 1000, "payment_mode": "UPI", "order_status": "DELIVERED",
            "delivery_minutes": 10,
        }])
        references = {
            "customers": pd.DataFrame([{"customer_id": "C-1"}]),
            "products": pd.DataFrame([{"product_id": "P-1", "mrp": 10}]),
            "stores": pd.DataFrame([{"store_id": "S-1"}]),
        }

        result = validate_frame("orders.csv", orders, references, pd.Timestamp("2026-10-19T00:00:00Z"))
        by_name = {check["check_name"]: check for check in result.checks}

        self.assertEqual(by_name["orders_volume_min_500"]["status"], "WARN")
        self.assertEqual(by_name["orders_unit_price_distribution"]["status"], "WARN")
        self.assertEqual(by_name["orders_freshness_within_one_day"]["status"], "WARN")
        self.assertEqual(len(result.passed), 1)

    def test_empty_orders_frame_returns_empty_split_and_scorecard(self):
        columns = [
            "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
            "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "delivery_minutes",
        ]
        result = validate_frame(
            "orders.csv", pd.DataFrame(columns=columns), {}, pd.Timestamp("2026-10-19T00:00:00Z")
        )

        self.assertTrue(result.passed.empty)
        self.assertTrue(result.failed.empty)
        self.assertGreater(len(result.checks), 0)

    def test_payment_reconciliation_captures_mismatch_orphan_and_missing_payment(self):
        orders = pd.DataFrame([
            {"order_id": "O-1", "order_status": "DELIVERED", "order_amount": 100.0},
            {"order_id": "O-2", "order_status": "DELIVERED", "order_amount": 50.0},
        ])
        payments = pd.DataFrame([
            {"payment_id": "P-1", "order_id": "O-1", "payment_status": "SUCCESS", "amount_paid": 90.0},
            {"payment_id": "P-2", "order_id": "O-404", "payment_status": "SUCCESS", "amount_paid": 10.0},
        ])

        reconciliation = build_payment_reconciliation(orders, payments)

        reasons = set(reconciliation["reason"])
        self.assertEqual(len(reconciliation), 3)
        self.assertEqual(reasons, {"amount_mismatch", "missing_success_payment", "unknown_order"})

    def test_reporting_frames_mask_pii_and_derive_revenue_margin_and_sla(self):
        orders = pd.DataFrame([{
            "order_id": "O-1", "order_ts": "2026-09-01 10:00:00", "customer_id": "C-1",
            "store_id": "S-1", "product_id": "P-1", "quantity": 2, "unit_price": 10,
            "discount_pct": 10, "order_amount": 18, "payment_mode": "upi", "order_status": "delivered",
            "delivery_minutes": 12, "channel": "App",
        }])
        customers = pd.DataFrame([{
            "customer_id": "C-1", "email": "priya@example.com", "phone": "9876543210",
            "loyalty_tier": "gold", "city": "Bengaluru", "area": "North", "pincode": "560001",
            "signup_date": "2025-01-01", "is_active": True,
        }])
        products = pd.DataFrame([{
            "product_id": "P-1", "product_name": "Milk", "category": "Dairy & Bakery", "cost_price": 5,
        }])
        stores = pd.DataFrame([{
            "store_id": "S-1", "store_name": "North 1", "zone": "North",
        }])

        tables = build_reporting_frames(orders, customers, products, stores)
        fact = tables["etl_fact_orders"].iloc[0]

        self.assertEqual(fact["email_masked"], "pr***@example.com")
        self.assertEqual(fact["phone_masked"], "98******10")
        self.assertEqual(fact["net_revenue"], 18.0)
        self.assertEqual(fact["gross_margin"], 8.0)
        self.assertTrue(fact["within_15_min"])


if __name__ == "__main__":
    unittest.main()
