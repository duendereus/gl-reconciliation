"""Tests for the break detection engine."""

from datetime import datetime

import pandas as pd
import pytest

from backend.services.reconciliation import (
    Break,
    detect_aml_flag,
    detect_duplicates,
    detect_fee_mismatch,
    detect_fx_rate,
    detect_interest_mismatch,
    detect_missing_counterparty,
    detect_settlement_timeout,
    detect_spei_duplicate,
    detect_unauthorized_reversal,
    extract_flagged_breaks,
    run_reconciliation,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _base_row(**overrides) -> dict:
    """Return a single transaction row with sensible defaults."""
    row = {
        "txn_id": "TXN-0001",
        "type": "FX_TRANSFER",
        "amount_usd": 1000.0,
        "amount_mxn": 17190.0,
        "rate_applied": 17.19,
        "rate_reference": 17.19,
        "timestamp": "2026-04-12T10:00:00",
        "status": "COMPLETED",
        "counterparty": "BBVA Mexico",
        "client_tier": "CORPORATE_T1",
        "client_id": "CLI-0001",
        "break_flag": "",
        "notes": "",
    }
    row.update(overrides)
    return row


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ===== FX Rate ===============================================================

class TestDetectFxRate:
    def test_no_break_when_rates_match(self):
        df = _df([_base_row(rate_applied=17.19, rate_reference=17.19)])
        assert detect_fx_rate(df) == []

    def test_no_break_within_threshold(self):
        df = _df([_base_row(rate_applied=17.195, rate_reference=17.19)])
        assert detect_fx_rate(df) == []

    def test_break_above_threshold(self):
        df = _df([_base_row(rate_applied=17.23, rate_reference=17.19)])
        breaks = detect_fx_rate(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "FX_RATE"
        assert breaks[0].details["rate_delta"] == 0.04

    def test_ignores_savings_accounts(self):
        df = _df([_base_row(
            type="SAVINGS_ACCOUNT",
            rate_applied=0.085,
            rate_reference=0.085,
        )])
        assert detect_fx_rate(df) == []

    def test_severity_scales_with_delta(self):
        df = _df([_base_row(rate_applied=17.35, rate_reference=17.19)])
        breaks = detect_fx_rate(df)
        assert breaks[0].severity == "Critical"

    def test_custom_threshold(self):
        df = _df([_base_row(rate_applied=17.195, rate_reference=17.19)])
        # With a tighter threshold, this should flag
        breaks = detect_fx_rate(df, threshold=0.001)
        assert len(breaks) == 1


# ===== Missing Counterparty ==================================================

class TestDetectMissingCounterparty:
    def test_no_break_for_completed(self):
        df = _df([_base_row(status="COMPLETED")])
        assert detect_missing_counterparty(df) == []

    def test_no_break_when_under_threshold(self):
        ref = datetime(2026, 4, 12, 12, 0, 0)
        df = _df([_base_row(
            status="DISPATCHED",
            timestamp="2026-04-12T10:00:00",
        )])
        assert detect_missing_counterparty(df, reference_time=ref) == []

    def test_break_when_over_48h(self):
        ref = datetime(2026, 4, 14, 12, 0, 0)  # 50h later
        df = _df([_base_row(
            status="DISPATCHED",
            timestamp="2026-04-12T10:00:00",
            type="SWIFT_INTERNATIONAL",
        )])
        breaks = detect_missing_counterparty(df, reference_time=ref)
        assert len(breaks) == 1
        assert breaks[0].break_type == "MISSING_COUNTERPARTY"
        assert breaks[0].details["hours_elapsed"] == 50.0

    def test_severity_high_under_72h(self):
        ref = datetime(2026, 4, 14, 15, 0, 0)  # 53h
        df = _df([_base_row(status="DISPATCHED", timestamp="2026-04-12T10:00:00")])
        breaks = detect_missing_counterparty(df, reference_time=ref)
        assert breaks[0].severity == "High"

    def test_severity_critical_over_72h(self):
        ref = datetime(2026, 4, 16, 0, 0, 0)  # 86h
        df = _df([_base_row(status="DISPATCHED", timestamp="2026-04-12T10:00:00")])
        breaks = detect_missing_counterparty(df, reference_time=ref)
        assert breaks[0].severity == "Critical"


# ===== Duplicates =============================================================

class TestDetectDuplicates:
    def test_no_duplicates(self):
        df = _df([
            _base_row(txn_id="TXN-001", counterparty="BBVA", amount_usd=100, timestamp="2026-04-12T10:00:00"),
            _base_row(txn_id="TXN-002", counterparty="HSBC", amount_usd=100, timestamp="2026-04-12T10:00:00"),
        ])
        assert detect_duplicates(df) == []

    def test_detects_duplicate_pair(self):
        row = _base_row(counterparty="BBVA", amount_usd=500, timestamp="2026-04-12T10:00:00")
        df = _df([
            {**row, "txn_id": "TXN-001"},
            {**row, "txn_id": "TXN-002"},
        ])
        breaks = detect_duplicates(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "DUPLICATE"
        assert set(breaks[0].details["duplicate_txn_ids"]) == {"TXN-001", "TXN-002"}

    def test_different_amounts_not_duplicate(self):
        df = _df([
            _base_row(txn_id="TXN-001", counterparty="BBVA", amount_usd=500, timestamp="2026-04-12T10:00:00"),
            _base_row(txn_id="TXN-002", counterparty="BBVA", amount_usd=501, timestamp="2026-04-12T10:00:00"),
        ])
        assert detect_duplicates(df) == []


# ===== Interest Mismatch =====================================================

class TestDetectInterestMismatch:
    def test_no_break_when_amount_matches(self):
        # amount_usd=1.0 * BANXICO_RATE(17.19) = 17.19 MXN
        df = _df([_base_row(
            type="SAVINGS_ACCOUNT",
            amount_usd=1.0,
            amount_mxn=17.19,
            rate_applied=0.085,
            rate_reference=0.085,
        )])
        assert detect_interest_mismatch(df) == []

    def test_break_when_delta_exceeds_threshold(self):
        df = _df([_base_row(
            type="SAVINGS_ACCOUNT",
            amount_usd=1.0,
            amount_mxn=17.50,  # delta = 0.31
            rate_applied=0.085,
            rate_reference=0.085,
        )])
        breaks = detect_interest_mismatch(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "INTEREST_MISMATCH"
        assert breaks[0].details["delta_mxn"] == 0.31

    def test_ignores_non_savings(self):
        df = _df([_base_row(
            type="FX_TRANSFER",
            amount_usd=1000,
            amount_mxn=20000,  # Big delta but not savings
        )])
        assert detect_interest_mismatch(df) == []

    def test_severity_low_under_1mxn(self):
        df = _df([_base_row(
            type="SAVINGS_ACCOUNT",
            amount_usd=1.0,
            amount_mxn=17.60,  # delta 0.41
        )])
        breaks = detect_interest_mismatch(df)
        assert breaks[0].severity == "Low"

    def test_severity_medium_over_1mxn(self):
        df = _df([_base_row(
            type="SAVINGS_ACCOUNT",
            amount_usd=1.0,
            amount_mxn=19.00,  # delta 1.81
        )])
        breaks = detect_interest_mismatch(df)
        assert breaks[0].severity == "Medium"


# ===== AML Flag ===============================================================

class TestDetectAmlFlag:
    def test_no_flag_for_normal_counterparty(self):
        df = _df([_base_row(counterparty="BBVA Mexico", amount_mxn=17190)])
        assert detect_aml_flag(df) == []

    def test_flags_unregistered_beneficiary_with_high_amount(self):
        df = _df([
            _base_row(txn_id="TXN-001", counterparty="BBVA Mexico", amount_mxn=10000, type="SWIFT_INTERNATIONAL"),
            _base_row(txn_id="TXN-002", counterparty="BBVA Mexico", amount_mxn=12000, type="SWIFT_INTERNATIONAL"),
            _base_row(txn_id="TXN-003", counterparty="Unregistered Beneficiary", amount_mxn=500000, type="SWIFT_INTERNATIONAL"),
        ])
        breaks = detect_aml_flag(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "AML_FLAG"
        assert breaks[0].severity == "Critical"

    def test_no_flag_when_amount_is_normal(self):
        df = _df([
            _base_row(txn_id="TXN-001", counterparty="BBVA Mexico", amount_mxn=10000, type="SWIFT_INTERNATIONAL"),
            _base_row(txn_id="TXN-002", counterparty="Unregistered Beneficiary", amount_mxn=10000, type="SWIFT_INTERNATIONAL"),
        ])
        # amount equals median, ratio = 1.0, not > 3.0
        assert detect_aml_flag(df) == []


# ===== Unauthorized Reversal ==================================================

class TestDetectUnauthorizedReversal:
    def test_no_break_for_positive_amounts(self):
        df = _df([_base_row(amount_mxn=17190)])
        assert detect_unauthorized_reversal(df) == []

    def test_flags_negative_without_matching_original(self):
        df = _df([_base_row(txn_id="TXN-001", amount_mxn=-5000, counterparty="BBVA")])
        breaks = detect_unauthorized_reversal(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "UNAUTHORIZED_REVERSAL"

    def test_no_flag_when_original_exists(self):
        df = _df([
            _base_row(txn_id="TXN-001", amount_mxn=5000, counterparty="BBVA"),
            _base_row(txn_id="TXN-002", amount_mxn=-5000, counterparty="BBVA"),
        ])
        # The reversal matches the original amount+counterparty
        assert detect_unauthorized_reversal(df) == []


# ===== Fee Mismatch ===========================================================

class TestDetectFeeMismatch:
    def test_no_break_without_flag(self):
        df = _df([_base_row(break_flag="")])
        assert detect_fee_mismatch(df) == []

    def test_detects_flagged_fee_mismatch(self):
        df = _df([_base_row(
            break_flag="FEE_MISMATCH",
            notes="Applied fee 1.2% vs contracted 0.9%",
        )])
        breaks = detect_fee_mismatch(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "FEE_MISMATCH"


# ===== Settlement Timeout =====================================================

class TestDetectSettlementTimeout:
    def test_no_break_before_cutoff(self):
        df = _df([_base_row(status="DISPATCHED", timestamp="2026-04-12T14:00:00")])
        assert detect_settlement_timeout(df) == []

    def test_break_at_cutoff(self):
        df = _df([_base_row(status="DISPATCHED", timestamp="2026-04-12T15:00:00")])
        breaks = detect_settlement_timeout(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "SETTLEMENT_TIMEOUT"

    def test_break_after_cutoff(self):
        df = _df([_base_row(status="DISPATCHED", timestamp="2026-04-12T16:30:00")])
        breaks = detect_settlement_timeout(df)
        assert len(breaks) == 1

    def test_ignores_completed(self):
        df = _df([_base_row(status="COMPLETED", timestamp="2026-04-12T16:30:00")])
        assert detect_settlement_timeout(df) == []


# ===== SPEI Duplicate =========================================================

class TestDetectSpeiDuplicate:
    def test_no_break_for_non_spei(self):
        df = _df([_base_row(type="FX_TRANSFER")])
        assert detect_spei_duplicate(df) == []

    def test_detects_spei_retry_pair(self):
        df = _df([
            _base_row(txn_id="TXN-1104a", type="SPEI", amount_mxn=3786.89),
            _base_row(txn_id="TXN-1104b", type="SPEI", amount_mxn=3281.52),
        ])
        breaks = detect_spei_duplicate(df)
        assert len(breaks) == 1
        assert breaks[0].break_type == "SPEI_DUPLICATE"
        assert set(breaks[0].details["duplicate_txn_ids"]) == {"TXN-1104a", "TXN-1104b"}

    def test_single_spei_no_break(self):
        df = _df([_base_row(txn_id="TXN-1104", type="SPEI")])
        assert detect_spei_duplicate(df) == []


# ===== Flagged Break Extraction ===============================================

class TestExtractFlaggedBreaks:
    def test_empty_when_no_flags(self):
        df = _df([_base_row(break_flag="")])
        assert extract_flagged_breaks(df) == []

    def test_extracts_flagged_rows(self):
        df = _df([
            _base_row(txn_id="TXN-001", break_flag="AML_FLAG", notes="Suspicious", amount_mxn=500000),
            _base_row(txn_id="TXN-002", break_flag="", notes=""),
        ])
        breaks = extract_flagged_breaks(df)
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"
        assert breaks[0].break_type == "AML_FLAG"
        assert breaks[0].severity == "Critical"

    def test_no_column_returns_empty(self):
        df = pd.DataFrame({"txn_id": ["TXN-001"], "amount_mxn": [100]})
        assert extract_flagged_breaks(df) == []


# ===== Orchestrator ===========================================================

class TestRunReconciliation:
    def test_deduplicates_flagged_and_rule_based(self):
        """Flagged breaks take precedence; rule-detected duplicates are skipped."""
        df = _df([_base_row(
            txn_id="TXN-4821",
            rate_applied=17.23,
            rate_reference=17.19,
            break_flag="FX_RATE",
            notes="Rate delta: 0.04",
        )])
        breaks = run_reconciliation(df)
        # Should appear only once despite both flag AND rule detecting it
        fx_breaks = [b for b in breaks if b.txn_id == "TXN-4821"]
        assert len(fx_breaks) == 1

    def test_combines_multiple_break_types(self):
        df = _df([
            _base_row(txn_id="TXN-001", rate_applied=17.30, rate_reference=17.19),
            _base_row(txn_id="TXN-002", amount_mxn=-5000, counterparty="BBVA"),
        ])
        ref = datetime(2026, 4, 12, 12, 0, 0)
        breaks = run_reconciliation(df, reference_time=ref)
        types = {b.break_type for b in breaks}
        assert "FX_RATE" in types
        assert "UNAUTHORIZED_REVERSAL" in types

    def test_empty_dataframe(self):
        df = _df([_base_row()])
        breaks = run_reconciliation(df)
        # A clean transaction should produce no breaks
        assert len(breaks) == 0

    def test_with_real_dataset_1(self):
        """Smoke test: DS1 should detect at least the FX rate break."""
        df = pd.read_csv("comp_files/dataset_1.csv")
        breaks = run_reconciliation(df)
        types = {b.break_type for b in breaks}
        assert "FX_RATE" in types
        fx = [b for b in breaks if b.break_type == "FX_RATE"]
        assert fx[0].txn_id == "TXN-4821"

    def test_with_real_dataset_2(self):
        """Smoke test: DS2 has 11 flagged rows — all should appear."""
        df = pd.read_csv("comp_files/dataset_2.csv")
        breaks = run_reconciliation(df)
        types = {b.break_type for b in breaks}
        assert "FX_RATE" in types
        assert "AML_FLAG" in types
        assert "SPEI_DUPLICATE" in types
        assert "UNAUTHORIZED_REVERSAL" in types
        assert "FEE_MISMATCH" in types
        assert "SETTLEMENT_TIMEOUT" in types
        assert "MISSING_COUNTERPARTY" in types
        assert "DUPLICATE" in types
        # At least the 11 flagged + any rule-only detections
        assert len(breaks) >= 11
