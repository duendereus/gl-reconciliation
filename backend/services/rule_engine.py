"""Dynamic rule evaluator.

Applies user-defined business rules (from the DB) against a transaction
DataFrame.  Each rule is a declarative condition:  field + operator + value,
with optional type/status filters and a compare_field for deltas.

This runs *after* the built-in detectors so custom rules can extend detection
without touching code.
"""

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from backend.models import BusinessRule
from backend.services.reconciliation import Break

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Operator implementations
# ---------------------------------------------------------------------------


def _cast_numeric(val: str) -> float:
    return float(val)


def _apply_rule(df: pd.DataFrame, rule: BusinessRule) -> list[Break]:
    """Evaluate a single rule against *df* and return any breaks found."""
    breaks: list[Break] = []
    subset = df.copy()

    # ── Optional filters ──
    if rule.filter_type:
        subset = subset[subset["type"] == rule.filter_type]
    if rule.filter_status:
        subset = subset[subset["status"] == rule.filter_status]
    if subset.empty:
        return breaks

    # ── Check that the target field exists ──
    if rule.field not in subset.columns:
        logger.warning("Rule %r: field '%s' not in CSV columns", rule.name, rule.field)
        return breaks

    op = rule.operator

    # ── delta_gt: |field - compare_field| > value ──
    if op == "delta_gt":
        if not rule.compare_field or rule.compare_field not in subset.columns:
            logger.warning("Rule %r: compare_field '%s' missing", rule.name, rule.compare_field)
            return breaks
        threshold = _cast_numeric(rule.value)
        subset = subset.copy()
        subset["_delta"] = (
            pd.to_numeric(subset[rule.field], errors="coerce")
            - pd.to_numeric(subset[rule.compare_field], errors="coerce")
        ).abs()
        flagged = subset[subset["_delta"] > threshold]

    # ── gt / lt: numeric comparison ──
    elif op in ("gt", "lt"):
        threshold = _cast_numeric(rule.value)
        col = pd.to_numeric(subset[rule.field], errors="coerce")
        if op == "gt":
            flagged = subset[col > threshold]
        else:
            flagged = subset[col < threshold]

    # ── eq / neq: string equality ──
    elif op in ("eq", "neq"):
        col = subset[rule.field].astype(str).str.strip()
        if op == "eq":
            flagged = subset[col == rule.value.strip()]
        else:
            flagged = subset[col != rule.value.strip()]

    # ── contains / not_contains: substring match ──
    elif op in ("contains", "not_contains"):
        col = subset[rule.field].astype(str)
        if op == "contains":
            flagged = subset[col.str.contains(rule.value, case=False, na=False)]
        else:
            flagged = subset[~col.str.contains(rule.value, case=False, na=False)]

    else:
        logger.warning("Rule %r: unknown operator '%s'", rule.name, op)
        return breaks

    # ── Build Break objects ──
    for _, row in flagged.iterrows():
        impact = abs(float(row.get("amount_mxn", 0)))
        details: dict = {"rule_id": rule.id, "rule_name": rule.name}
        if op == "delta_gt":
            details["delta"] = round(float(row["_delta"]), 4)
            details[rule.field] = float(row[rule.field])
            details[rule.compare_field] = float(row[rule.compare_field])

        breaks.append(Break(
            txn_id=str(row["txn_id"]),
            break_type=rule.break_type,
            description=f"[{rule.name}] {rule.description}",
            impact_mxn=round(impact, 2),
            severity=rule.severity,
            details=details,
        ))

    return breaks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def evaluate_custom_rules(
    df: pd.DataFrame,
    rules: list[BusinessRule],
    exclude_txn_ids: set[str] | None = None,
) -> list[Break]:
    """Run all active custom rules against *df*.

    Parameters
    ----------
    df : DataFrame
        Transaction data.
    rules : list[BusinessRule]
        Active rules from the database.
    exclude_txn_ids : set, optional
        Transaction IDs already flagged by built-in detectors — skip these
        to avoid duplicates.
    """
    exclude = exclude_txn_ids or set()
    all_breaks: list[Break] = []

    for rule in rules:
        if not rule.is_active:
            continue
        try:
            results = _apply_rule(df, rule)
            for b in results:
                if b.txn_id not in exclude:
                    all_breaks.append(b)
                    exclude.add(b.txn_id)
        except Exception:
            logger.exception("Error evaluating rule %r", rule.name)

    return all_breaks
