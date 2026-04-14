import os
from pathlib import Path

import pandas as pd
from fastapi import APIRouter

router = APIRouter()

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "comp_files"

DATASET_META = {
    "dataset_1": {
        "filename": "dataset_1.csv",
        "label": "Dataset 1 — Apr 12, 2026",
        "description": "200 synthetic transactions",
    },
    "dataset_2": {
        "filename": "dataset_2.csv",
        "label": "Dataset 2 — Apr 11, 2026",
        "description": "500 synthetic transactions",
    },
}


@router.get("")
async def list_datasets():
    """Return available datasets with row counts."""
    results = []
    for key, meta in DATASET_META.items():
        filepath = DATA_DIR / meta["filename"]
        row_count = 0
        if filepath.exists():
            df = pd.read_csv(filepath)
            row_count = len(df)
        results.append(
            {
                "id": key,
                "label": meta["label"],
                "description": meta["description"],
                "transactions": row_count,
                "path": str(filepath),
            }
        )
    return {"datasets": results}
