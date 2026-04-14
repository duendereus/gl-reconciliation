"""Break detection engine for GL reconciliation.

Detects discrepancies in transaction CSVs using configurable rules.
Two detection paths:
  1. Explicit flags — rows with a populated ``break_flag`` column.
  2. Rule-based    — algorithmic detection (FX rate, missing counterparty, …).

Results from both paths are merged and deduplicated by ``txn_id``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Sequence

import pandas as pd

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BANXICO_RATE = 17.19  # Hardcoded reference FX rate (MXN/USD)

FX_RATE_THRESHOLD = 0.01          # MXN/USD delta to flag
INTEREST_DELTA_THRESHOLD = 0.10   # MXN absolute delta
MISSING_COUNTERPARTY_HOURS = 48   # Hours without bank confirmation
SETTLEMENT_CUTOFF_HOUR = 15       # 15:00 h local — SWIFT settlement window
AML_VELOCITY_MULTIPLIER = 3.0     # Flag if amount > 3× median for counterparty


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Break:
    txn_id: str
    break_type: str
    description: str
    impact_mxn: float
    severity: str  # Low | Medium | High | Critical
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Individual rule detectors
# ---------------------------------------------------------------------------

def detect_fx_rate(df: pd.DataFrame, threshold: float = FX_RATE_THRESHOLD) -> list[Break]:
    """Flag transactions where applied FX rate deviates from reference."""
    breaks: list[Break] = []
    # Only check non-SAVINGS rows (savings uses interest rate, not FX)
    mask = (
        (df["type"] != "SAVINGS_ACCOUNT")
        & df["rate_applied"].notna()
        & df["rate_reference"].notna()
    )
    subset = df.loc[mask].copy()
    subset["rate_delta"] = (subset["rate_applied"] - subset["rate_reference"]).abs()
    flagged = subset[subset["rate_delta"] > threshold]

    for _, row in flagged.iterrows():
        delta = round(row["rate_delta"], 4)
        breaks.append(Break(
            txn_id=str(row["txn_id"]),
            break_type="FX_RATE",
            description=(
                f"Applied rate {row['rate_applied']} vs reference {row['rate_reference']} "
                f"(delta {delta} MXN/USD)"
            ),
            impact_mxn=round(abs(row["amount_mxn"]), 2),
            severity=_fx_severity(delta),
            details={
                "rate_applied": float(row["rate_applied"]),
                "rate_reference": float(row["rate_reference"]),
                "rate_delta": delta,
            },
        ))
    return breaks


def detect_missing_counterparty(
    df: pd.DataFrame,
    reference_time: datetime | None = None,
    hours: int = MISSING_COUNTERPARTY_HOURS,
) -> list[Break]:
    """Flag DISPATCHED transactions with no bank confirmation after *hours*."""
    breaks: list[Break] = []
    if reference_time is None:
        reference_time = _max_timestamp(df)

    dispatched = df[df["status"] == "DISPATCHED"].copy()
    dispatched["ts"] = pd.to_datetime(dispatched["timestamp"])
    dispatched["hours_elapsed"] = (
        (reference_time - dispatched["ts"]).dt.total_seconds() / 3600
    )
    flagged = dispatched[dispatched["hours_elapsed"] > hours]

    for _, row in flagged.iterrows():
        elapsed = round(row["hours_elapsed"], 1)
        breaks.append(Break(
            txn_id=str(row["txn_id"]),
            break_type="MISSING_COUNTERPARTY",
            description=(
                f"SWIFT transaction dispatched {elapsed}h ago with no bank confirmation "
                f"(threshold: {hours}h). Counterparty: {row.get('counterparty', 'N/A')}"
            ),
            impact_mxn=round(abs(row["amount_mxn"]), 2),
            severity="High" if elapsed < 72 else "Critical",
            details={
                "hours_elapsed": elapsed,
                "counterparty": str(row.get("counterparty", "")),
                "status": "DISPATCHED",
            },
        ))
    return breaks


def detect_duplicates(df: pd.DataFrame) -> list[Break]:
    """Flag rows that share the same counterparty + amount_usd + timestamp."""
    breaks: list[Break] = []
    key_cols = ["counterparty", "amount_usd", "timestamp"]
    if not all(c in df.columns for c in key_cols):
        return breaks

    dupes = df[df.duplicated(subset=key_cols, keep=False)]
    # Group so we report each duplicate cluster once
    for _, group in dupes.groupby(key_cols):
        ids = group["txn_id"].tolist()
        impact = round(abs(group["amount_mxn"].iloc[0]), 2)
        breaks.append(Break(
            txn_id=str(ids[0]),
            break_type="DUPLICATE",
            description=(
                f"Duplicate: same counterparty/amount/timestamp across {len(ids)} "
                f"transactions ({', '.join(str(i) for i in ids)})"
            ),
            impact_mxn=impact,
            severity="Medium",
            details={"duplicate_txn_ids": [str(i) for i in ids]},
        ))
    return breaks


def detect_interest_mismatch(
    df: pd.DataFrame,
    threshold: float = INTEREST_DELTA_THRESHOLD,
) -> list[Break]:
    """Flag SAVINGS_ACCOUNT rows where credited MXN deviates from expected yield.

    For savings accounts the ``rate_applied`` / ``rate_reference`` columns hold the
    interest rate (e.g. 0.085 = 8.5%).  The expected MXN value is
    ``amount_usd × BANXICO_RATE``.  If the actual ``amount_mxn`` differs by more
    than *threshold* it indicates a calculation error on the credited interest.
    """
    breaks: list[Break] = []
    savings = df[df["type"] == "SAVINGS_ACCOUNT"].copy()
    if savings.empty:
        return breaks

    savings["expected_mxn"] = savings["amount_usd"] * BANXICO_RATE
    savings["interest_delta"] = (savings["amount_mxn"] - savings["expected_mxn"]).abs()
    flagged = savings[savings["interest_delta"] > threshold]

    for _, row in flagged.iterrows():
        delta = round(row["interest_delta"], 2)
        breaks.append(Break(
            txn_id=str(row["txn_id"]),
            break_type="INTEREST_MISMATCH",
            description=(
                f"Expected {round(row['expected_mxn'], 2)} MXN, "
                f"credited {row['amount_mxn']} MXN (delta {delta} MXN)"
            ),
            impact_mxn=delta,
            severity="Low" if delta < 1 else "Medium",
            details={
                "expected_mxn": round(row["expected_mxn"], 2),
                "actual_mxn": float(row["amount_mxn"]),
                "delta_mxn": delta,
            },
        ))
    return breaks


def detect_aml_flag(df: pd.DataFrame) -> list[Break]:
    """Flag transactions with suspicious AML indicators.

    Heuristic: DISPATCHED to an unregistered/unusual beneficiary AND amount
    significantly above the median for that transaction type.
    """
    breaks: list[Break] = []
    suspicious_counterparties = {"unregistered beneficiary"}

    for _, row in df.iterrows():
        cp = str(row.get("counterparty", "")).lower().strip()
        if cp in suspicious_counterparties:
            median_amount = df.loc[df["type"] == row["type"], "amount_mxn"].median()
            ratio = abs(row["amount_mxn"]) / median_amount if median_amount else 0
            if ratio > AML_VELOCITY_MULTIPLIER:
                breaks.append(Break(
                    txn_id=str(row["txn_id"]),
                    break_type="AML_FLAG",
                    description=(
                        f"Velocity {round(ratio * 100)}% above client profile threshold. "
                        f"Counterparty: {row['counterparty']}"
                    ),
                    impact_mxn=round(abs(row["amount_mxn"]), 2),
                    severity="Critical",
                    details={
                        "counterparty": str(row["counterparty"]),
                        "velocity_ratio": round(ratio, 2),
                    },
                ))
    return breaks


def detect_unauthorized_reversal(df: pd.DataFrame) -> list[Break]:
    """Flag negative-amount transactions with no matching original."""
    breaks: list[Break] = []
    negatives = df[df["amount_mxn"] < 0]
    for _, row in negatives.iterrows():
        # Look for a matching positive transaction with same absolute amount and counterparty
        match = df[
            (df["amount_mxn"] == abs(row["amount_mxn"]))
            & (df["counterparty"] == row["counterparty"])
            & (df["txn_id"] != row["txn_id"])
        ]
        if match.empty:
            breaks.append(Break(
                txn_id=str(row["txn_id"]),
                break_type="UNAUTHORIZED_REVERSAL",
                description=(
                    f"Reversal of {row['amount_mxn']} MXN with no matching original transaction. "
                    f"Counterparty: {row.get('counterparty', 'N/A')}"
                ),
                impact_mxn=round(abs(row["amount_mxn"]), 2),
                severity="High",
                details={"counterparty": str(row.get("counterparty", ""))},
            ))
    return breaks


def detect_fee_mismatch(df: pd.DataFrame) -> list[Break]:
    """Flag transactions where break_flag indicates FEE_MISMATCH.

    Fee mismatch requires external fee schedule data not present in the CSV, so
    this detector relies on the ``break_flag`` / ``notes`` columns when populated.
    """
    breaks: list[Break] = []
    if "break_flag" not in df.columns:
        return breaks

    flagged = df[df["break_flag"].str.upper().str.strip() == "FEE_MISMATCH"]
    for _, row in flagged.iterrows():
        breaks.append(Break(
            txn_id=str(row["txn_id"]),
            break_type="FEE_MISMATCH",
            description=str(row.get("notes", "Fee mismatch detected")),
            impact_mxn=round(abs(row["amount_mxn"]), 2),
            severity="Medium",
            details={"notes": str(row.get("notes", ""))},
        ))
    return breaks


def detect_settlement_timeout(df: pd.DataFrame) -> list[Break]:
    """Flag DISPATCHED transactions past the settlement cutoff hour."""
    breaks: list[Break] = []
    dispatched = df[df["status"].isin(["DISPATCHED", "PENDING"])].copy()
    dispatched["ts"] = pd.to_datetime(dispatched["timestamp"])

    for _, row in dispatched.iterrows():
        if row["ts"].hour >= SETTLEMENT_CUTOFF_HOUR:
            breaks.append(Break(
                txn_id=str(row["txn_id"]),
                break_type="SETTLEMENT_TIMEOUT",
                description=(
                    f"Dispatched at {row['ts'].strftime('%H:%M')}h — "
                    f"settlement window closed at {SETTLEMENT_CUTOFF_HOUR}:00h"
                ),
                impact_mxn=round(abs(row["amount_mxn"]), 2),
                severity="High",
                details={
                    "dispatch_hour": row["ts"].hour,
                    "cutoff_hour": SETTLEMENT_CUTOFF_HOUR,
                },
            ))
    return breaks


def detect_spei_duplicate(df: pd.DataFrame) -> list[Break]:
    """Flag SPEI transactions that appear to be retries (e.g. TXN-1104a / TXN-1104b)."""
    breaks: list[Break] = []
    spei = df[df["type"] == "SPEI"]
    if spei.empty:
        return breaks

    # Group by base txn_id (strip trailing letter suffix)
    spei = spei.copy()
    spei["base_id"] = spei["txn_id"].str.replace(r"[a-zA-Z]+$", "", regex=True)
    groups = spei.groupby("base_id").filter(lambda g: len(g) > 1)

    for base_id, group in groups.groupby("base_id"):
        ids = group["txn_id"].tolist()
        total_impact = round(group["amount_mxn"].sum(), 2)
        breaks.append(Break(
            txn_id=str(ids[0]),
            break_type="SPEI_DUPLICATE",
            description=(
                f"SPEI duplicate: {len(ids)} entries with same base ID "
                f"({', '.join(str(i) for i in ids)}). Likely network retry."
            ),
            impact_mxn=abs(total_impact),
            severity="Medium",
            details={"duplicate_txn_ids": [str(i) for i in ids]},
        ))
    return breaks


# ---------------------------------------------------------------------------
# Flagged-row extraction (for CSVs with pre-populated break_flag column)
# ---------------------------------------------------------------------------

_FLAG_SEVERITY: dict[str, str] = {
    "FX_RATE": "Medium",
    "MISSING_COUNTERPARTY": "High",
    "DUPLICATE": "Medium",
    "INTEREST_MISMATCH": "Low",
    "AML_FLAG": "Critical",
    "UNAUTHORIZED_REVERSAL": "High",
    "FEE_MISMATCH": "Medium",
    "SETTLEMENT_TIMEOUT": "High",
    "SPEI_DUPLICATE": "Medium",
}


def extract_flagged_breaks(df: pd.DataFrame) -> list[Break]:
    """Read rows that already have ``break_flag`` populated."""
    breaks: list[Break] = []
    if "break_flag" not in df.columns:
        return breaks

    flagged = df[df["break_flag"].notna() & (df["break_flag"].str.strip() != "")]
    for _, row in flagged.iterrows():
        btype = str(row["break_flag"]).strip().upper()
        notes = str(row.get("notes", "")) if pd.notna(row.get("notes")) else ""
        breaks.append(Break(
            txn_id=str(row["txn_id"]),
            break_type=btype,
            description=notes or f"{btype} detected",
            impact_mxn=round(abs(row["amount_mxn"]), 2),
            severity=_FLAG_SEVERITY.get(btype, "Medium"),
            details={"notes": notes, "source": "break_flag"},
        ))
    return breaks


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

ALL_RULE_DETECTORS = [
    detect_fx_rate,
    detect_missing_counterparty,
    detect_duplicates,
    detect_interest_mismatch,
    detect_aml_flag,
    detect_unauthorized_reversal,
    detect_fee_mismatch,
    detect_settlement_timeout,
    detect_spei_duplicate,
]


def run_reconciliation(
    df: pd.DataFrame,
    reference_time: datetime | None = None,
    detectors: Sequence | None = None,
) -> list[Break]:
    """Run all break detectors on *df* and return deduplicated results.

    Parameters
    ----------
    df : DataFrame
        Transaction data with standard columns.
    reference_time : datetime, optional
        "Now" for elapsed-time calculations.  Defaults to the max timestamp
        in the dataset.
    detectors : sequence, optional
        Override the list of rule detectors to run.
    """
    if detectors is None:
        detectors = ALL_RULE_DETECTORS

    if reference_time is None:
        reference_time = _max_timestamp(df)

    # 1. Extract explicitly-flagged breaks
    all_breaks = extract_flagged_breaks(df)
    seen_ids: set[str] = {b.txn_id for b in all_breaks}

    # 2. Run rule-based detectors
    for detector in detectors:
        try:
            # Some detectors accept extra kwargs
            if detector is detect_missing_counterparty:
                results = detector(df, reference_time=reference_time)
            else:
                results = detector(df)
        except Exception:
            # Don't let one bad rule crash the whole pipeline
            continue

        for b in results:
            if b.txn_id not in seen_ids:
                all_breaks.append(b)
                seen_ids.add(b.txn_id)

    return all_breaks


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _max_timestamp(df: pd.DataFrame) -> datetime:
    """Return the latest timestamp in the dataset."""
    ts = pd.to_datetime(df["timestamp"])
    return ts.max().to_pydatetime()


def _fx_severity(delta: float) -> str:
    if delta > 0.10:
        return "Critical"
    if delta > 0.05:
        return "High"
    if delta > 0.02:
        return "Medium"
    return "Low"
