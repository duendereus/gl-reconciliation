"""Tests for CSV schema validation."""

import pandas as pd
import pytest

from backend.services.csv_validator import (
    REQUIRED_COLUMNS,
    ValidationError,
    validate_csv,
)


def _valid_row(**overrides) -> dict:
    row = {
        "txn_id": "TXN-001",
        "type": "FX_TRANSFER",
        "amount_usd": 1000.0,
        "amount_mxn": 17190.0,
        "rate_applied": 17.19,
        "rate_reference": 17.19,
        "timestamp": "2026-04-12T10:00:00",
        "status": "COMPLETED",
        "counterparty": "BBVA Mexico",
    }
    row.update(overrides)
    return row


def test_valid_csv_passes():
    df = pd.DataFrame([_valid_row()])
    result = validate_csv(df)
    assert len(result) == 1
    assert result.iloc[0]["txn_id"] == "TXN-001"


def test_empty_dataframe_rejected():
    df = pd.DataFrame()
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    assert "empty" in str(exc.value).lower()


def test_missing_required_column():
    row = _valid_row()
    del row["rate_applied"]
    df = pd.DataFrame([row])
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    assert "rate_applied" in str(exc.value)
    assert "Missing required" in str(exc.value)


def test_missing_multiple_columns():
    row = _valid_row()
    del row["rate_applied"]
    del row["timestamp"]
    df = pd.DataFrame([row])
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    msg = str(exc.value)
    assert "rate_applied" in msg
    assert "timestamp" in msg


def test_non_numeric_amount_rejected():
    df = pd.DataFrame([_valid_row(amount_usd="not-a-number")])
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    assert "amount_usd" in str(exc.value)
    assert "numbers" in str(exc.value).lower()


def test_invalid_timestamp_rejected():
    df = pd.DataFrame([_valid_row(timestamp="bad-date")])
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    assert "timestamp" in str(exc.value)


def test_missing_txn_id_rejected():
    df = pd.DataFrame([_valid_row(txn_id=None)])
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    assert "transaction ID" in str(exc.value) or "txn_id" in str(exc.value)


def test_too_many_rows_rejected():
    # Build 10001 rows efficiently
    rows = [_valid_row(txn_id=f"TXN-{i}") for i in range(10_001)]
    df = pd.DataFrame(rows)
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    assert "exceeds" in str(exc.value).lower() or "limit" in str(exc.value).lower()


def test_optional_columns_filled_if_missing():
    row = _valid_row()
    df = pd.DataFrame([row])
    result = validate_csv(df)
    assert "break_flag" in result.columns
    assert "notes" in result.columns


def test_numeric_coercion_preserves_valid_values():
    df = pd.DataFrame([_valid_row(amount_usd="1000.50")])
    result = validate_csv(df)
    assert result.iloc[0]["amount_usd"] == 1000.50


def test_error_has_helpful_hint():
    row = _valid_row()
    del row["txn_id"]
    df = pd.DataFrame([row])
    with pytest.raises(ValidationError) as exc:
        validate_csv(df)
    err = exc.value
    assert err.hint  # non-empty hint
