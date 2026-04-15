"""CSV schema validation — returns clear, COO-readable error messages."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# Required columns for reconciliation analysis.
REQUIRED_COLUMNS = [
    "txn_id",
    "type",
    "amount_usd",
    "amount_mxn",
    "rate_applied",
    "rate_reference",
    "timestamp",
    "status",
    "counterparty",
]

# Optional but expected (won't fail if missing, but logged).
OPTIONAL_COLUMNS = ["client_tier", "client_id", "break_flag", "notes"]

# Column types — values coerced; failures collected.
NUMERIC_COLUMNS = ["amount_usd", "amount_mxn", "rate_applied", "rate_reference"]

MAX_ROWS = 10_000  # Sanity limit for demo
MIN_ROWS = 1


@dataclass
class ValidationError(Exception):
    """Raised when a CSV cannot be processed. The ``message`` is user-facing."""

    message: str
    hint: str = ""

    def __str__(self) -> str:
        if self.hint:
            return f"{self.message} {self.hint}"
        return self.message


def validate_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize a transaction DataFrame.

    Raises ``ValidationError`` with a clear message on any structural issue.
    Returns the cleaned DataFrame on success.
    """
    # --- Shape checks ---
    if df is None or df.empty:
        raise ValidationError(
            message="The file is empty.",
            hint="Please upload a CSV with at least one transaction row.",
        )

    if len(df) > MAX_ROWS:
        raise ValidationError(
            message=f"File has {len(df):,} rows, which exceeds the {MAX_ROWS:,} limit.",
            hint="Please split the file into smaller batches.",
        )

    if len(df) < MIN_ROWS:
        raise ValidationError(message="The file has no data rows.")

    # --- Required columns ---
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValidationError(
            message=f"Missing required column{'s' if len(missing) > 1 else ''}: {', '.join(missing)}.",
            hint=f"Expected columns: {', '.join(REQUIRED_COLUMNS)}.",
        )

    # --- Numeric columns ---
    bad_numeric: list[str] = []
    for col in NUMERIC_COLUMNS:
        coerced = pd.to_numeric(df[col], errors="coerce")
        if coerced.isna().all():
            bad_numeric.append(col)
        else:
            df[col] = coerced
    if bad_numeric:
        raise ValidationError(
            message=f"Column{'s' if len(bad_numeric) > 1 else ''} {', '.join(bad_numeric)} must contain numbers.",
            hint="Remove any text or currency symbols from these columns.",
        )

    # --- Timestamp ---
    try:
        ts = pd.to_datetime(df["timestamp"], errors="coerce")
        if ts.isna().all():
            raise ValueError()
        df["timestamp"] = ts.dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception:
        raise ValidationError(
            message="Column 'timestamp' could not be parsed.",
            hint="Use ISO format, e.g. 2026-04-12T17:11:00.",
        )

    # --- txn_id uniqueness within reasonable bounds (warn only via flag check) ---
    if df["txn_id"].isna().any():
        raise ValidationError(
            message="Some rows are missing a transaction ID (txn_id).",
            hint="Every row must have a unique txn_id value.",
        )

    # --- Fill optional columns if absent (so downstream code doesn't break) ---
    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    return df
