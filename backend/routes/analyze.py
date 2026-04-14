from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import BusinessRule
from backend.services.claude_client import BreakAnalysis, analyze_breaks
from backend.services.reconciliation import run_reconciliation
from backend.services.rule_engine import evaluate_custom_rules

router = APIRouter()

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "comp_files"


@router.post("")
async def analyze(
    file: UploadFile | None = File(None),
    dataset_id: str | None = Form(None),
    db: Session = Depends(get_db),
):
    """Analyze a CSV for reconciliation breaks.

    Accepts either:
      - An uploaded CSV file, or
      - A dataset_id referencing a bundled dataset (dataset_1, dataset_2).
    """
    if file and file.filename:
        contents = await file.read()
        try:
            df = pd.read_csv(io.BytesIO(contents))
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid CSV: {exc}")
    elif dataset_id:
        filepath = DATA_DIR / f"{dataset_id}.csv"
        if not filepath.exists():
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
        df = pd.read_csv(filepath)
    else:
        raise HTTPException(status_code=400, detail="Provide a CSV file or dataset_id")

    # 1. Built-in break detection
    breaks = run_reconciliation(df)

    # 2. Custom rules from DB
    custom_rules = db.query(BusinessRule).filter(BusinessRule.is_active == True).all()
    if custom_rules:
        seen_ids = {b.txn_id for b in breaks}
        custom_breaks = evaluate_custom_rules(df, custom_rules, exclude_txn_ids=seen_ids)
        breaks.extend(custom_breaks)

    # Build row lookup for richer Claude prompts
    row_lookup: dict[str, dict] = {}
    for _, row in df.iterrows():
        txn_id = str(row.get("txn_id", ""))
        if txn_id:
            row_lookup[txn_id] = {
                k: (v if not pd.isna(v) else None)
                for k, v in row.to_dict().items()
            }

    # Claude AI analysis (with DB cache)
    analyses = await analyze_breaks(breaks, row_lookup=row_lookup, db=db)
    analysis_dicts = [a.to_dict() for a in analyses]

    total_impact = sum(b.impact_mxn for b in breaks)
    manual_minutes_per_break = 45
    time_saved_h = round(len(breaks) * manual_minutes_per_break / 60, 1)

    # Chart data computed from full CSV
    chart_data = _build_chart_data(df, breaks)

    return {
        "status": "ok",
        "transactions": len(df),
        "columns": list(df.columns),
        "breaks": [b.to_dict() for b in breaks],
        "analyses": analysis_dicts,
        "chart_data": chart_data,
        "summary": {
            "total_transactions": len(df),
            "breaks_found": len(breaks),
            "unreconciled_amount": f"${total_impact:,.2f}",
            "time_saved": f"{time_saved_h}h",
        },
    }


def _build_chart_data(df: pd.DataFrame, breaks) -> dict:
    """Compute chart-ready data from the full transaction DataFrame."""
    # Type distribution
    type_order = ["FX_TRANSFER", "SWIFT_INTERNATIONAL", "CORPORATE_CARD", "SAVINGS_ACCOUNT", "SPEI"]
    type_counts = df["type"].value_counts()
    type_data = [int(type_counts.get(t, 0)) for t in type_order]
    type_labels = ["FX Transfer", "SWIFT Int'l", "Corporate Card", "Savings", "SPEI"]

    # Hourly distribution
    hours = pd.to_datetime(df["timestamp"]).dt.hour
    hour_counts = hours.value_counts().sort_index()
    hour_data = [int(hour_counts.get(h, 0)) for h in range(24)]

    # Cumulative amount by type (MXN)
    bar_amounts = df.groupby("type")["amount_mxn"].sum()
    bar_data = [round(float(bar_amounts.get(t, 0)), 0) for t in type_order]

    # Scatter outliers from breaks
    outliers = [
        {"x": i + 1, "y": round(b.impact_mxn, 0)}
        for i, b in enumerate(breaks)
    ]

    return {
        "type_labels": type_labels,
        "type_data": type_data,
        "hour_data": hour_data,
        "bar_labels": type_labels,
        "bar_data": bar_data,
        "outliers": outliers,
        "scatter_normal_count": max(len(df) - len(breaks), 0),
    }
