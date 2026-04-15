"""Admin endpoints — inspect DB state for debugging."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import AnalysisCache, BusinessRule, LoginEvent, SavedDataset, User

router = APIRouter()


@router.get("/stats")
def db_stats(db: Session = Depends(get_db)):
    """Summary counts + cache efficiency metrics."""
    from sqlalchemy import func

    cached = db.query(AnalysisCache).count()
    # Total breaks analyzed across all uploads (sum of break_count)
    total_breaks = db.query(func.coalesce(func.sum(SavedDataset.break_count), 0)).scalar() or 0
    # Claude calls avoided = every break beyond the first unique one
    calls_avoided = max(total_breaks - cached, 0)
    hit_rate = round((calls_avoided / total_breaks * 100), 1) if total_breaks else 0.0
    # Rough cost estimate — Sonnet pricing (~$3/MTok input, ~$15/MTok output)
    # Each analysis ~800 in + ~400 out tokens = ~$0.0084 per call
    cost_per_call = 0.0084
    cost_saved_usd = round(calls_avoided * cost_per_call, 2)

    return {
        "users": db.query(User).count(),
        "saved_datasets": db.query(SavedDataset).count(),
        "analysis_cache": cached,
        "business_rules": db.query(BusinessRule).count(),
        "login_events": db.query(LoginEvent).count(),
        "cache_efficiency": {
            "total_breaks_processed": int(total_breaks),
            "unique_cached": cached,
            "claude_calls_avoided": int(calls_avoided),
            "cache_hit_rate_pct": hit_rate,
            "estimated_cost_saved_usd": cost_saved_usd,
        },
    }


@router.get("/cache")
def list_cache(db: Session = Depends(get_db)):
    """List all cached AI analyses (grouped by txn_id)."""
    entries = db.query(AnalysisCache).order_by(AnalysisCache.created_at.desc()).all()
    return {
        "total": len(entries),
        "entries": [
            {
                "id": e.id,
                "txn_id": e.txn_id,
                "cache_key": e.cache_key[:16] + "...",
                "created_at": e.created_at.isoformat() if e.created_at else None,
                "size_bytes": len(e.analysis_json),
            }
            for e in entries
        ],
    }


@router.get("/datasets")
def list_saved_datasets(db: Session = Depends(get_db)):
    """Verbose list of saved datasets with sizes."""
    datasets = db.query(SavedDataset).order_by(SavedDataset.created_at.desc()).all()
    return {
        "total": len(datasets),
        "datasets": [
            {
                "id": d.id,
                "name": d.name,
                "transactions": d.transaction_count,
                "breaks": d.break_count,
                "size_kb": round(
                    (len(d.breaks_json) + len(d.analyses_json) + len(d.chart_data_json)) / 1024, 1
                ),
                "created_at": d.created_at.isoformat() if d.created_at else None,
            }
            for d in datasets
        ],
    }
