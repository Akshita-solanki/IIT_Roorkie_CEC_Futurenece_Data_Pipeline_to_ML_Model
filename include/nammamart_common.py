"""Shared NammaMart normalization and privacy helpers."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any

import pandas as pd


_PAYMENT_MODES = {
    "upi": "UPI",
    "card": "Card",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "wallet": "Wallet",
}
_STATUSES = {"delivered": "DELIVERED", "cancelled": "CANCELLED", "returned": "RETURNED"}
_TIERS = {"bronze": "Bronze", "silver": "Silver", "gold": "Gold", "platinum": "Platinum"}


def pipeline_status_from_task_states(states: dict[str, Any]) -> str:
    """Return FAILED when any task in an Airflow run failed or was blocked."""
    failed_states = {"failed", "upstream_failed"}
    if any(str(state).lower().split(".")[-1] in failed_states for state in states.values()):
        return "FAILED"
    return "SUCCESS"


def validate_csv_headers(path: str | Path, required_columns: set[str]) -> None:
    """Fail early when a source CSV is missing a required schema column."""
    with open(path, newline="", encoding="utf-8-sig") as source:
        actual_columns = set(next(csv.reader(source), []))
    missing = sorted(required_columns - actual_columns)
    if missing:
        raise ValueError(f"{Path(path).name} is missing required columns: {', '.join(missing)}")


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def normalize_text(value: Any, field: str | None = None) -> Any:
    """Trim strings and canonicalize known categorical values."""
    if _is_missing(value):
        return None
    if not isinstance(value, str):
        return value

    cleaned = value.strip()
    if not cleaned:
        return None
    field = (field or "").lower()
    key = cleaned.casefold()
    if field == "payment_mode":
        return _PAYMENT_MODES.get(key, cleaned)
    if field == "order_status":
        return _STATUSES.get(key, cleaned)
    if field == "loyalty_tier":
        return _TIERS.get(key, cleaned)
    if field in {"zone", "channel"}:
        return cleaned.title()
    return cleaned


def mask_email(value: Any) -> str | None:
    """Mask the local part while retaining a minimally useful domain."""
    if _is_missing(value):
        return None
    email = str(value).strip()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email):
        return "***"
    local, domain = email.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(value: Any) -> str | None:
    """Return a masked ten-digit phone or a generic mask for malformed data."""
    if _is_missing(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) != 10:
        return "***"
    return f"{digits[:2]}{'*' * (len(digits) - 4)}{digits[-2:]}"


def normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with whitespace and known categorical variants normalized."""
    normalized = frame.copy()
    for column in normalized.columns:
        normalized[column] = normalized[column].map(
            lambda value, col=column: normalize_text(value, field=col)
        )
    return normalized


def upsert_orders(
    current: pd.DataFrame,
    incoming: pd.DataFrame,
    key: str = "order_id",
) -> tuple[pd.DataFrame, int, int]:
    """Apply an order snapshot by key and report changed inserts/updates.

    Rows with null keys are rejected because their identity cannot be merged.
    The most recent incoming row wins for a key, making repeat loads stable.
    """
    if key not in current.columns or key not in incoming.columns:
        raise ValueError(f"Both frames must contain {key}")
    if incoming[key].isna().any():
        raise ValueError(f"Incoming rows must have a non-null {key}")
    columns = list(dict.fromkeys([*current.columns, *incoming.columns]))
    old = current.reindex(columns=columns).copy()
    new = incoming.reindex(columns=columns).copy().drop_duplicates(key, keep="last")
    old = old.drop_duplicates(key, keep="last")
    current_by_key = old.set_index(key, drop=False)
    existing_keys = set(current_by_key.index)
    inserted = sum(order_key not in existing_keys for order_key in new[key])

    def same_value(left: Any, right: Any) -> bool:
        if _is_missing(left) and _is_missing(right):
            return True
        if _is_missing(left) or _is_missing(right):
            return False
        return left == right

    updated = 0
    for _, row in new.iterrows():
        order_key = row[key]
        if order_key in existing_keys:
            previous = current_by_key.loc[order_key]
            if isinstance(previous, pd.DataFrame):
                previous = previous.iloc[-1]
            if any(not same_value(previous[column], row[column]) for column in columns if column != key):
                updated += 1

    incoming_keys = set(new[key])
    merged = pd.concat([old.loc[~old[key].isin(incoming_keys)], new], ignore_index=True)
    return merged.drop_duplicates(key, keep="last").reset_index(drop=True), inserted, updated
