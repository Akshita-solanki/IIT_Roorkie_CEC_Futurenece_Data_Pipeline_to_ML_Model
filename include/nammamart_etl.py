"""Pure transformation functions used by the NammaMart ETL DAG."""

from __future__ import annotations

from typing import Any

import pandas as pd

from nammamart_common import mask_email, mask_phone, normalize_frame, normalize_text, upsert_orders


def build_reporting_frames(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Create masked dimensions and a fact frame from quality-passing rows."""
    orders = normalize_frame(orders)
    customers = normalize_frame(customers).drop_duplicates("customer_id", keep="last")
    products = normalize_frame(products).drop_duplicates("product_id", keep="last")
    stores = normalize_frame(stores).drop_duplicates("store_id", keep="last")

    dim_customer = pd.DataFrame({
        "customer_id": customers["customer_id"],
        "email_masked": customers.get("email", pd.Series(index=customers.index, dtype="object")).map(mask_email),
        "phone_masked": customers.get("phone", pd.Series(index=customers.index, dtype="object")).map(mask_phone),
        "city": customers.get("city"),
        "area": customers.get("area"),
        "pincode": customers.get("pincode"),
        "signup_date": customers.get("signup_date"),
        "loyalty_tier": customers.get("loyalty_tier"),
        "is_active": customers.get("is_active"),
    }).drop_duplicates("customer_id", keep="last")
    dim_product = products[[
        column for column in ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"] if column in products
    ]].copy()
    dim_store = stores[[
        column for column in ["store_id", "store_name", "zone", "area", "pincode", "open_date", "capacity_orders_per_day"] if column in stores
    ]].copy()

    merged = orders.merge(dim_customer, on="customer_id", how="inner", suffixes=("", "_customer"))
    merged = merged.merge(dim_product, on="product_id", how="inner", suffixes=("", "_product"))
    merged = merged.merge(dim_store, on="store_id", how="inner", suffixes=("", "_store"))
    order_time = pd.to_datetime(merged["order_ts"], errors="coerce", format="mixed")
    merged["order_ts"] = order_time
    merged["order_date"] = order_time.dt.strftime("%Y-%m-%d")
    delivered = merged["order_status"].astype("string").str.upper().eq("DELIVERED")
    amount = pd.to_numeric(merged["order_amount"], errors="coerce").fillna(0)
    quantity = pd.to_numeric(merged["quantity"], errors="coerce").fillna(0)
    cost = pd.to_numeric(merged["cost_price"], errors="coerce").fillna(0)
    merged["net_revenue"] = amount.where(delivered, 0.0).round(2)
    merged["gross_margin"] = (merged["net_revenue"] - quantity * cost).round(2)
    delivery = pd.to_numeric(merged["delivery_minutes"], errors="coerce")
    merged["within_15_min"] = delivered & delivery.le(15)

    fact_columns = [
        "order_id", "order_ts", "order_date", "customer_id", "email_masked", "phone_masked",
        "store_id", "store_name", "zone", "product_id", "product_name", "category",
        "quantity", "unit_price", "discount_pct", "order_amount", "net_revenue",
        "gross_margin", "payment_mode", "order_status", "delivery_minutes", "within_15_min", "channel",
    ]
    fact = merged[[column for column in fact_columns if column in merged]].copy()
    return {
        "etl_fact_orders": fact,
        "etl_dim_customer": dim_customer,
        "etl_dim_product": dim_product,
        "etl_dim_store": dim_store,
    }


def build_payment_reconciliation(
    orders: pd.DataFrame,
    payments: pd.DataFrame,
    all_orders: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Reconcile clean orders without mistaking rejected order keys for orphans.

    ``orders`` is the quality-passing population eligible for reporting.
    ``all_orders`` supplies source keys only, so payments for rejected orders are
    not mislabeled as payments for an unknown order.
    """
    order_rows = normalize_frame(orders)
    payment_rows = normalize_frame(payments)
    order_rows = order_rows.drop_duplicates("order_id", keep="last")
    payment_rows = payment_rows.drop_duplicates("payment_id", keep="last")
    orders_by_id = order_rows.set_index("order_id", drop=False)
    known_ids = set(orders_by_id.index)
    if all_orders is not None and "order_id" in all_orders:
        known_ids.update(normalize_frame(all_orders)["order_id"].dropna().tolist())
    successful = payment_rows[
        payment_rows["payment_status"].astype("string").str.upper().eq("SUCCESS")
    ]
    rows: list[dict[str, Any]] = []

    for _, payment in payment_rows.iterrows():
        order_id = payment.get("order_id")
        if pd.isna(order_id) or order_id not in known_ids:
            rows.append({
                "order_id": order_id,
                "payment_id": payment.get("payment_id"),
                "order_amount": None,
                "amount_paid": payment.get("amount_paid"),
                "difference": None,
                "reason": "unknown_order",
            })

    successful_by_order = {order_id: group for order_id, group in successful.groupby("order_id", dropna=False)}
    for order_id, order in orders_by_id.iterrows():
        status = str(order.get("order_status", "")).upper()
        group = successful_by_order.get(order_id)
        if status != "DELIVERED":
            if group is not None:
                for _, payment in group.iterrows():
                    rows.append({
                        "order_id": order_id,
                        "payment_id": payment.get("payment_id"),
                        "order_amount": order.get("order_amount"),
                        "amount_paid": payment.get("amount_paid"),
                        "difference": None,
                        "reason": "successful_payment_for_non_delivered_order",
                    })
            continue
        if group is None or group.empty:
            rows.append({
                "order_id": order_id,
                "payment_id": None,
                "order_amount": order.get("order_amount"),
                "amount_paid": None,
                "difference": None,
                "reason": "missing_success_payment",
            })
            continue
        order_amount = pd.to_numeric(pd.Series([order.get("order_amount")]), errors="coerce").iloc[0]
        if len(group) > 1:
            for _, payment in group.iterrows():
                paid = pd.to_numeric(pd.Series([payment.get("amount_paid")]), errors="coerce").iloc[0]
                rows.append({
                    "order_id": order_id,
                    "payment_id": payment.get("payment_id"),
                    "order_amount": order_amount,
                    "amount_paid": paid,
                    "difference": round(float(paid - order_amount), 2) if pd.notna(paid) and pd.notna(order_amount) else None,
                    "reason": "multiple_success_payments",
                })
            continue
        payment = group.iloc[0]
        paid = pd.to_numeric(pd.Series([payment.get("amount_paid")]), errors="coerce").iloc[0]
        if pd.isna(paid) or pd.isna(order_amount) or abs(float(paid - order_amount)) > 0.01:
            rows.append({
                "order_id": order_id,
                "payment_id": payment.get("payment_id"),
                "order_amount": order_amount,
                "amount_paid": paid,
                "difference": round(float(paid - order_amount), 2) if pd.notna(paid) and pd.notna(order_amount) else None,
                "reason": "amount_mismatch",
            })
    return pd.DataFrame(rows, columns=["order_id", "payment_id", "order_amount", "amount_paid", "difference", "reason"])
