"""Dataset endpoints — list and retrieve saved datasets from DB."""

from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import SavedDataset

router = APIRouter()


@router.get("")
def list_datasets(db: Session = Depends(get_db)):
    """Return all saved datasets (most recent first)."""
    datasets = db.query(SavedDataset).order_by(SavedDataset.created_at.desc()).all()
    return {"datasets": [d.to_dict() for d in datasets]}


@router.get("/{dataset_id}")
def get_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """Return full saved results for a dataset."""
    ds = db.query(SavedDataset).filter(SavedDataset.id == dataset_id).first()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return {
        "dataset_id": ds.id,
        "name": ds.name,
        "transactions": ds.transaction_count,
        "breaks": json.loads(ds.breaks_json),
        "analyses": json.loads(ds.analyses_json),
        "chart_data": json.loads(ds.chart_data_json),
        "summary": json.loads(ds.summary_json),
    }
