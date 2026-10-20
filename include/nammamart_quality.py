"""Severity-aware Python quality rules for the NammaMart ETL path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from nammamart_common import normalize_frame


@dataclass
class ValidationResult:
    passed: pd.DataFrame
    failed: pd.DataFrame
    checks: list[dict[str, Any]]


def _date_series(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_datetime(frame[column], errors="coerce", format="mixed")


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce")


def _key_set(frame: pd.DataFrame | None, key: str) -> set[str]:
    if frame is None or key not in frame:
        return set()
    return set(frame[key].dropna().astype(str).str.strip())


def _run_timestamp(run_at: datetime | pd.Timestamp) -> pd.Timestamp:
    stamp = pd.Timestamp(run_at)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("Asia/Kolkata").tz_localize(None)
    return stamp


def validate_frame(
    file_name: str,
    frame: pd.DataFrame,
    reference_frames: dict[str, pd.DataFrame] | None,
    run_at: datetime | pd.Timestamp,
) -> ValidationResult:
    """Split source rows by critical rules and emit one scorecard row per check.

    Warnings remain in ``passed``. Critical failures are represented in
    ``failed`` with every applicable reason joined in ``failure_reason``.
    Missing required columns fail loudly so schema changes cannot pass silently.
    """
    references = reference_frames or {}
    expected = {
        "orders.csv": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "delivery_minutes"],
        "orders_2026-10-01_increment.csv": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "delivery_minutes"],
        "payments.csv": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status"],
        "customers.csv": ["customer_id", "loyalty_tier"],
        "products.csv": ["product_id", "category", "mrp", "cost_price"],
        "stores.csv": ["store_id", "zone", "capacity_orders_per_day"],
    }
    if file_name not in expected:
        raise ValueError(f"Unsupported NammaMart source file: {file_name}")
    missing = sorted(set(expected[file_name]) - set(frame.columns))
    if missing:
        raise ValueError(f"{file_name} is missing required columns: {', '.join(missing)}")

    original = frame.copy().reset_index(drop=True)
    clean = normalize_frame(original)
    n = len(clean)
    critical = pd.Series(False, index=clean.index)
    reasons: list[list[str]] = [[] for _ in range(n)]
    checks: list[dict[str, Any]] = []
    checked_at = pd.Timestamp(run_at).isoformat()
    now = _run_timestamp(run_at)

    def record(name: str, dimension: str, severity: str, mask: Any, reason: str) -> None:
        mask = pd.Series(mask, index=clean.index).fillna(False).astype(bool)
        failed_count = int(mask.sum())
        if severity == "Critical":
            critical.loc[mask] = True
            for row_number in clean.index[mask]:
                if reason not in reasons[row_number]:
                    reasons[row_number].append(reason)
        checks.append({
            "check_name": name,
            "dimension": dimension,
            "severity": severity,
            "rows_checked": n,
            "rows_failed": failed_count,
            "status": "FAIL" if failed_count and severity == "Critical" else ("WARN" if failed_count else "PASS"),
            "checked_at": checked_at,
        })

    def required(columns: list[str], prefix: str) -> None:
        mask = clean[columns].isna().any(axis=1)
        record(f"{prefix}_required_fields", "completeness", "Critical", mask, f"missing required value: {', '.join(columns)}")

    def unique(key: str, prefix: str, reject_all_conflicting_rows: bool = False) -> None:
        duplicate_rows = clean[key].duplicated(keep=False) if reject_all_conflicting_rows else clean[key].duplicated(keep="last")
        record(f"{prefix}_{key}_unique", "uniqueness", "Critical", duplicate_rows, f"duplicate {key}")

    prefix = file_name.replace(".csv", "").replace("-", "_")
    if file_name in {"orders.csv", "orders_2026-10-01_increment.csv"}:
        required(["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status"], prefix)
        unique("order_id", prefix, reject_all_conflicting_rows=True)
        status = clean["order_status"].astype("string").str.upper()
        record(f"{prefix}_status_allowed", "validity", "Critical", ~status.isin(["DELIVERED", "CANCELLED", "RETURNED"]), "invalid order_status")
        mode = clean["payment_mode"].astype("string").str.casefold()
        record(f"{prefix}_payment_mode_allowed", "validity", "Critical", ~mode.isin(["upi", "card", "cash on delivery", "wallet"]), "invalid payment_mode")
        dt = _date_series(clean, "order_ts")
        record(f"{prefix}_timestamp_parseable", "validity", "Critical", dt.isna(), "unparseable order_ts")
        future = dt > now
        record(f"{prefix}_no_future_order", "timeliness", "Critical", future, "future order_ts")
        qty = _numeric(clean, "quantity")
        price = _numeric(clean, "unit_price")
        discount = _numeric(clean, "discount_pct")
        amount = _numeric(clean, "order_amount")
        record(f"{prefix}_discount_range", "validity", "Critical", ~discount.between(0, 100), "discount_pct outside 0–100")
        record(f"{prefix}_quantity_positive", "accuracy", "Critical", qty.le(0), "quantity must be positive")
        expected_amount = (qty * price * (1 - discount / 100)).round(2)
        record(f"{prefix}_amount_formula", "accuracy", "Critical", (expected_amount - amount).abs().gt(0.01), "order_amount does not match quantity × unit_price × discount")
        delivery = _numeric(clean, "delivery_minutes")
        delivered = status.eq("DELIVERED")
        record(f"{prefix}_delivered_has_delivery_time", "completeness", "Critical", delivered & delivery.isna(), "DELIVERED order missing delivery_minutes")
        record(f"{prefix}_delivery_below_120", "accuracy", "Critical", delivery.ge(120), "delivery_minutes must be below 120")
        for column, dimension in [("customer_id", "customers"), ("product_id", "products"), ("store_id", "stores")]:
            valid = _key_set(references.get(dimension), column)
            known = clean[column].astype("string").str.strip().isin(valid)
            record(f"{prefix}_{column}_exists", "consistency", "Critical", clean[column].notna() & ~known, f"unknown {column}")
        product_reference = references.get("products")
        if product_reference is not None and {"product_id", "mrp"}.issubset(product_reference.columns):
            mrp_by_product = product_reference.drop_duplicates("product_id", keep="last").set_index("product_id")["mrp"]
            mrp = pd.to_numeric(clean["product_id"].map(mrp_by_product), errors="coerce")
            price_drift = mrp.notna() & mrp.gt(0) & (_numeric(clean, "unit_price") > mrp * 5)
            record(f"{prefix}_unit_price_distribution", "accuracy", "Warning", price_drift, "unit_price exceeds 5x product MRP")
        checks.append({
            "check_name": f"{prefix}_volume_min_500",
            "dimension": "completeness",
            "severity": "Warning",
            "rows_checked": n,
            "rows_failed": int(n < 500),
            "status": "WARN" if n < 500 else "PASS",
            "checked_at": checked_at,
        })
        valid_dates = dt.dropna()
        valid_dates = valid_dates[valid_dates <= now]
        stale = bool(len(valid_dates) and valid_dates.max() < now - pd.Timedelta(days=1))
        checks.append({
            "check_name": f"{prefix}_freshness_within_one_day",
            "dimension": "timeliness",
            "severity": "Warning",
            "rows_checked": 1,
            "rows_failed": int(stale),
            "status": "WARN" if stale else "PASS",
            "checked_at": checked_at,
        })

    elif file_name == "payments.csv":
        required(["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status"], prefix)
        unique("payment_id", prefix)
        mode = clean["payment_mode"].astype("string").str.casefold()
        record(f"{prefix}_payment_mode_allowed", "validity", "Critical", ~mode.isin(["upi", "card", "cash on delivery", "wallet"]), "invalid payment_mode")
        record(f"{prefix}_payment_status_allowed", "validity", "Critical", ~clean["payment_status"].astype("string").str.upper().isin(["SUCCESS", "FAILED", "REFUNDED"]), "invalid payment_status")
        dt = _date_series(clean, "payment_ts")
        record(f"{prefix}_timestamp_parseable", "validity", "Critical", dt.isna(), "unparseable payment_ts")
        record(f"{prefix}_no_future_payment", "timeliness", "Critical", dt > now, "future payment_ts")
        record(f"{prefix}_amount_nonnegative", "accuracy", "Critical", _numeric(clean, "amount_paid").lt(0), "amount_paid must be non-negative")
        order_keys = _key_set(references.get("orders"), "order_id")
        record(f"{prefix}_order_exists", "consistency", "Critical", clean["order_id"].notna() & ~clean["order_id"].astype("string").str.strip().isin(order_keys), "payment references unknown order_id")
        if "gateway_ref" in clean:
            record(f"{prefix}_gateway_ref_present", "completeness", "Warning", clean["gateway_ref"].isna(), "gateway_ref missing")
        order_frame = references.get("orders")
        if order_frame is not None and {"order_id", "order_amount"}.issubset(order_frame.columns):
            amounts = order_frame.drop_duplicates("order_id", keep="last").set_index("order_id")["order_amount"]
            expected_amount = clean["order_id"].map(amounts)
            mismatch = clean["payment_status"].astype("string").str.upper().eq("SUCCESS") & (_numeric(clean, "amount_paid") - pd.to_numeric(expected_amount, errors="coerce")).abs().gt(0.01)
            record(f"{prefix}_successful_amount_matches", "consistency", "Critical", mismatch, "SUCCESS payment amount mismatch")

    elif file_name == "customers.csv":
        required(["customer_id", "loyalty_tier"], prefix)
        unique("customer_id", prefix)
        if "email" in clean:
            email = clean["email"].astype("string")
            malformed = email.notna() & ~email.str.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", na=False)
            record(f"{prefix}_email_valid", "validity", "Warning", clean["email"].isna() | malformed, "email missing or invalid")
        if "phone" in clean:
            phone = clean["phone"].astype("string").str.replace(r"\D", "", regex=True)
            record(f"{prefix}_phone_valid", "validity", "Warning", phone.str.len().ne(10), "phone is not ten digits")
        if "pincode" in clean:
            pincode = clean["pincode"].astype("string").str.strip()
            record(f"{prefix}_pincode_valid", "validity", "Warning", ~pincode.str.fullmatch(r"[1-9]\d{5}", na=False), "pincode is not a valid six-digit Indian PIN code")
        tier = clean["loyalty_tier"].astype("string").str.casefold()
        record(f"{prefix}_loyalty_tier_allowed", "validity", "Critical", ~tier.isin(["bronze", "silver", "gold", "platinum"]), "invalid loyalty_tier")
        record(f"{prefix}_loyalty_tier_casing", "consistency", "Warning", clean["loyalty_tier"].notna() & clean["loyalty_tier"].astype("string").ne(clean["loyalty_tier"].astype("string").str.title()), "loyalty_tier casing normalized")
        if "signup_date" in clean:
            signup = _date_series(clean, "signup_date")
            record(f"{prefix}_signup_date_parseable", "validity", "Critical", signup.isna(), "unparseable signup_date")
            record(f"{prefix}_no_future_signup", "timeliness", "Critical", signup > now, "future signup_date")

    elif file_name == "products.csv":
        required(["product_id", "category", "mrp", "cost_price"], prefix)
        unique("product_id", prefix)
        record(f"{prefix}_mrp_positive", "accuracy", "Critical", _numeric(clean, "mrp").le(0), "mrp must be positive")
        record(f"{prefix}_cost_price_valid", "accuracy", "Critical", _numeric(clean, "cost_price").le(0) | (_numeric(clean, "cost_price") > _numeric(clean, "mrp")), "cost_price must be positive and <= mrp")
        if "launch_date" in clean:
            launched = _date_series(clean, "launch_date")
            record(f"{prefix}_launch_date_parseable", "validity", "Critical", launched.isna(), "unparseable launch_date")
            record(f"{prefix}_no_future_launch", "timeliness", "Critical", launched > now, "future launch_date")

    elif file_name == "stores.csv":
        required(["store_id", "zone", "capacity_orders_per_day"], prefix)
        unique("store_id", prefix)
        record(f"{prefix}_capacity_positive", "accuracy", "Critical", _numeric(clean, "capacity_orders_per_day").le(0), "capacity_orders_per_day must be positive")
        if "open_date" in clean:
            opened = _date_series(clean, "open_date")
            record(f"{prefix}_open_date_parseable", "validity", "Critical", opened.isna(), "unparseable open_date")
            record(f"{prefix}_no_future_open", "timeliness", "Critical", opened > now, "future open_date")

    failed_mask = critical
    failed = original.loc[failed_mask].copy()
    failed["failure_reason"] = ["; ".join(reasons[i]) for i in failed.index]
    passed = original.loc[~failed_mask].copy()
    return ValidationResult(passed=passed, failed=failed, checks=checks)
