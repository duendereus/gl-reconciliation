"""Tests for business rules CRUD and dynamic rule evaluation."""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.database import Base, get_db
from backend.main import app
from backend.models import BusinessRule
from backend.services.rule_engine import evaluate_custom_rules

# ---------------------------------------------------------------------------
# Test DB setup
# ---------------------------------------------------------------------------

TEST_DB_URL = "sqlite:///./test_rules.db"
engine = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
TestSession = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def override_get_db():
    db = TestSession()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db


@pytest.fixture(autouse=True)
def setup_db():
    """Create tables before each test, drop after."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_rule(**overrides) -> dict:
    payload = {
        "name": "Test Rule",
        "description": "A test rule",
        "break_type": "CUSTOM",
        "severity": "Medium",
        "field": "amount_mxn",
        "operator": "gt",
        "value": "100000",
        "compare_field": None,
        "filter_type": None,
        "filter_status": None,
        "is_active": True,
    }
    payload.update(overrides)
    return payload


def _base_row(**overrides) -> dict:
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


# ===== CRUD Endpoints ========================================================

class TestRulesCRUD:
    def test_list_empty(self):
        res = client.get("/rules")
        assert res.status_code == 200
        assert res.json()["rules"] == []

    def test_create_rule(self):
        res = client.post("/rules", json=_create_rule(name="High Amount"))
        assert res.status_code == 201
        data = res.json()
        assert data["name"] == "High Amount"
        assert data["id"] is not None
        assert data["is_active"] is True

    def test_get_rule(self):
        create = client.post("/rules", json=_create_rule())
        rule_id = create.json()["id"]
        res = client.get(f"/rules/{rule_id}")
        assert res.status_code == 200
        assert res.json()["name"] == "Test Rule"

    def test_get_rule_not_found(self):
        res = client.get("/rules/999")
        assert res.status_code == 404

    def test_update_rule(self):
        create = client.post("/rules", json=_create_rule())
        rule_id = create.json()["id"]
        res = client.put(f"/rules/{rule_id}", json={"name": "Updated Rule", "severity": "High"})
        assert res.status_code == 200
        assert res.json()["name"] == "Updated Rule"
        assert res.json()["severity"] == "High"

    def test_update_rule_not_found(self):
        res = client.put("/rules/999", json={"name": "X"})
        assert res.status_code == 404

    def test_delete_rule(self):
        create = client.post("/rules", json=_create_rule())
        rule_id = create.json()["id"]
        res = client.delete(f"/rules/{rule_id}")
        assert res.status_code == 204
        # Verify gone
        res2 = client.get(f"/rules/{rule_id}")
        assert res2.status_code == 404

    def test_delete_rule_not_found(self):
        res = client.delete("/rules/999")
        assert res.status_code == 404

    def test_list_after_create(self):
        client.post("/rules", json=_create_rule(name="Rule A"))
        client.post("/rules", json=_create_rule(name="Rule B"))
        res = client.get("/rules")
        rules = res.json()["rules"]
        assert len(rules) == 2
        names = {r["name"] for r in rules}
        assert names == {"Rule A", "Rule B"}

    def test_invalid_operator_rejected(self):
        res = client.post("/rules", json=_create_rule(operator="invalid_op"))
        assert res.status_code == 422

    def test_invalid_severity_rejected(self):
        res = client.post("/rules", json=_create_rule(severity="SuperHigh"))
        assert res.status_code == 422


class TestSeedRules:
    def test_seed_creates_defaults(self):
        res = client.post("/rules/seed")
        assert res.status_code == 201
        data = res.json()
        assert len(data["seeded"]) > 0
        # Verify rules exist
        rules = client.get("/rules").json()["rules"]
        assert len(rules) >= 9

    def test_seed_is_idempotent(self):
        client.post("/rules/seed")
        res = client.post("/rules/seed")
        assert res.status_code == 201
        assert res.json()["skipped"] > 0
        assert len(res.json()["seeded"]) == 0


# ===== Rule Engine ============================================================

class TestRuleEngine:
    def _make_rule(self, **kwargs) -> BusinessRule:
        defaults = {
            "id": 1,
            "name": "Test Rule",
            "description": "test",
            "break_type": "CUSTOM",
            "severity": "Medium",
            "field": "amount_mxn",
            "operator": "gt",
            "value": "100000",
            "compare_field": None,
            "filter_type": None,
            "filter_status": None,
            "is_active": True,
        }
        defaults.update(kwargs)
        r = BusinessRule()
        for k, v in defaults.items():
            setattr(r, k, v)
        return r

    def test_gt_operator(self):
        rule = self._make_rule(field="amount_mxn", operator="gt", value="50000")
        df = _df([
            _base_row(txn_id="TXN-001", amount_mxn=60000),
            _base_row(txn_id="TXN-002", amount_mxn=30000),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"

    def test_lt_operator(self):
        rule = self._make_rule(field="amount_mxn", operator="lt", value="0")
        df = _df([
            _base_row(txn_id="TXN-001", amount_mxn=-5000),
            _base_row(txn_id="TXN-002", amount_mxn=3000),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"

    def test_eq_operator(self):
        rule = self._make_rule(field="status", operator="eq", value="DISPATCHED")
        df = _df([
            _base_row(txn_id="TXN-001", status="DISPATCHED"),
            _base_row(txn_id="TXN-002", status="COMPLETED"),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"

    def test_neq_operator(self):
        rule = self._make_rule(field="status", operator="neq", value="COMPLETED")
        df = _df([
            _base_row(txn_id="TXN-001", status="DISPATCHED"),
            _base_row(txn_id="TXN-002", status="COMPLETED"),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1

    def test_delta_gt_operator(self):
        rule = self._make_rule(
            field="rate_applied",
            operator="delta_gt",
            value="0.01",
            compare_field="rate_reference",
        )
        df = _df([
            _base_row(txn_id="TXN-001", rate_applied=17.25, rate_reference=17.19),
            _base_row(txn_id="TXN-002", rate_applied=17.19, rate_reference=17.19),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"
        assert breaks[0].details["delta"] == 0.06

    def test_contains_operator(self):
        rule = self._make_rule(field="counterparty", operator="contains", value="Unregistered")
        df = _df([
            _base_row(txn_id="TXN-001", counterparty="Unregistered Beneficiary"),
            _base_row(txn_id="TXN-002", counterparty="BBVA Mexico"),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1

    def test_not_contains_operator(self):
        rule = self._make_rule(field="counterparty", operator="not_contains", value="BBVA")
        df = _df([
            _base_row(txn_id="TXN-001", counterparty="HSBC Mexico"),
            _base_row(txn_id="TXN-002", counterparty="BBVA Mexico"),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"

    def test_filter_type(self):
        rule = self._make_rule(
            field="amount_mxn", operator="gt", value="50000",
            filter_type="SWIFT_INTERNATIONAL",
        )
        df = _df([
            _base_row(txn_id="TXN-001", type="SWIFT_INTERNATIONAL", amount_mxn=60000),
            _base_row(txn_id="TXN-002", type="FX_TRANSFER", amount_mxn=60000),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"

    def test_filter_status(self):
        rule = self._make_rule(
            field="amount_mxn", operator="gt", value="10000",
            filter_status="DISPATCHED",
        )
        df = _df([
            _base_row(txn_id="TXN-001", status="DISPATCHED", amount_mxn=50000),
            _base_row(txn_id="TXN-002", status="COMPLETED", amount_mxn=50000),
        ])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-001"

    def test_inactive_rule_skipped(self):
        rule = self._make_rule(is_active=False, field="amount_mxn", operator="gt", value="0")
        df = _df([_base_row()])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 0

    def test_exclude_txn_ids(self):
        rule = self._make_rule(field="amount_mxn", operator="gt", value="0")
        df = _df([
            _base_row(txn_id="TXN-001", amount_mxn=5000),
            _base_row(txn_id="TXN-002", amount_mxn=5000),
        ])
        breaks = evaluate_custom_rules(df, [rule], exclude_txn_ids={"TXN-001"})
        assert len(breaks) == 1
        assert breaks[0].txn_id == "TXN-002"

    def test_missing_field_returns_empty(self):
        rule = self._make_rule(field="nonexistent_column", operator="gt", value="0")
        df = _df([_base_row()])
        breaks = evaluate_custom_rules(df, [rule])
        assert len(breaks) == 0

    def test_multiple_rules(self):
        rule1 = self._make_rule(id=1, name="Rule 1", field="amount_mxn", operator="gt", value="50000")
        rule2 = self._make_rule(id=2, name="Rule 2", field="status", operator="eq", value="DISPATCHED")
        df = _df([
            _base_row(txn_id="TXN-001", amount_mxn=60000, status="COMPLETED"),
            _base_row(txn_id="TXN-002", amount_mxn=10000, status="DISPATCHED"),
        ])
        breaks = evaluate_custom_rules(df, [rule1, rule2])
        assert len(breaks) == 2
        ids = {b.txn_id for b in breaks}
        assert ids == {"TXN-001", "TXN-002"}

    def test_break_has_rule_metadata(self):
        rule = self._make_rule(id=42, name="My Rule", field="amount_mxn", operator="gt", value="0")
        df = _df([_base_row(txn_id="TXN-001", amount_mxn=5000)])
        breaks = evaluate_custom_rules(df, [rule])
        assert breaks[0].details["rule_id"] == 42
        assert breaks[0].details["rule_name"] == "My Rule"
        assert "[My Rule]" in breaks[0].description
