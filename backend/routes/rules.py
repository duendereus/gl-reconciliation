"""CRUD endpoints for business rules."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from backend.database import get_db
from backend.models import BusinessRule
from backend.routes.auth import require_write_access

router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class RuleCreate(BaseModel):
    name: str
    description: str = ""
    break_type: str
    severity: str = "Medium"
    field: str
    operator: str
    value: str
    compare_field: str | None = None
    filter_type: str | None = None
    filter_status: str | None = None
    is_active: bool = True


class RuleUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    break_type: str | None = None
    severity: str | None = None
    field: str | None = None
    operator: str | None = None
    value: str | None = None
    compare_field: str | None = None
    filter_type: str | None = None
    filter_status: str | None = None
    is_active: bool | None = None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

VALID_OPERATORS = {"gt", "lt", "eq", "neq", "delta_gt", "contains", "not_contains"}
VALID_SEVERITIES = {"Low", "Medium", "High", "Critical"}


def _validate_rule(data: RuleCreate | RuleUpdate) -> None:
    if data.operator is not None and data.operator not in VALID_OPERATORS:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid operator '{data.operator}'. Must be one of: {', '.join(sorted(VALID_OPERATORS))}",
        )
    if data.severity is not None and data.severity not in VALID_SEVERITIES:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid severity '{data.severity}'. Must be one of: {', '.join(sorted(VALID_SEVERITIES))}",
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("")
def list_rules(db: Session = Depends(get_db)):
    """Return all business rules."""
    rules = db.query(BusinessRule).order_by(BusinessRule.id).all()
    return {"rules": [r.to_dict() for r in rules]}


@router.get("/{rule_id}")
def get_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = db.query(BusinessRule).filter(BusinessRule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule.to_dict()


@router.post("", status_code=201)
def create_rule(data: RuleCreate, db: Session = Depends(get_db), _: None = Depends(require_write_access)):
    _validate_rule(data)
    rule = BusinessRule(**data.model_dump())
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule.to_dict()


@router.put("/{rule_id}")
def update_rule(rule_id: int, data: RuleUpdate, db: Session = Depends(get_db), _: None = Depends(require_write_access)):
    _validate_rule(data)
    rule = db.query(BusinessRule).filter(BusinessRule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    for key, val in data.model_dump(exclude_unset=True).items():
        setattr(rule, key, val)
    db.commit()
    db.refresh(rule)
    return rule.to_dict()


@router.delete("/{rule_id}", status_code=204)
def delete_rule(rule_id: int, db: Session = Depends(get_db), _: None = Depends(require_write_access)):
    rule = db.query(BusinessRule).filter(BusinessRule.id == rule_id).first()
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    db.delete(rule)
    db.commit()
    return None


def seed_default_rules(db: Session) -> dict:
    """Populate the DB with the built-in reconciliation rules.

    Idempotent: skips rules whose name already exists.
    Called at startup and via the /seed endpoint.
    """
    defaults = [
        RuleCreate(
            name="FX Rate Deviation",
            description="Flag when applied FX rate deviates from BANXICO reference rate",
            break_type="FX_RATE",
            severity="Medium",
            field="rate_applied",
            operator="delta_gt",
            value="0.01",
            compare_field="rate_reference",
            filter_type=None,
            filter_status=None,
        ),
        RuleCreate(
            name="Missing Counterparty (48h)",
            description="Flag DISPATCHED transactions with no bank confirmation after 48 hours",
            break_type="MISSING_COUNTERPARTY",
            severity="High",
            field="status",
            operator="eq",
            value="DISPATCHED",
            filter_type="SWIFT_INTERNATIONAL",
            filter_status="DISPATCHED",
        ),
        RuleCreate(
            name="Duplicate Transaction",
            description="Flag transactions with same counterparty, amount, and timestamp (built-in detector)",
            break_type="DUPLICATE",
            severity="Medium",
            field="counterparty",
            operator="eq",
            value="*",
            compare_field=None,
            filter_type=None,
            is_active=True,
        ),
        RuleCreate(
            name="Interest Mismatch",
            description="Flag savings accounts where credited MXN deviates from expected yield (built-in detector)",
            break_type="INTEREST_MISMATCH",
            severity="Low",
            field="amount_mxn",
            operator="delta_gt",
            value="0.10",
            compare_field=None,
            filter_type="SAVINGS_ACCOUNT",
            is_active=True,
        ),
        RuleCreate(
            name="AML Velocity Flag",
            description="Flag transactions to unregistered beneficiaries exceeding 3x median amount",
            break_type="AML_FLAG",
            severity="Critical",
            field="counterparty",
            operator="contains",
            value="Unregistered",
            filter_type=None,
        ),
        RuleCreate(
            name="Unauthorized Reversal",
            description="Flag negative-amount transactions with no matching original",
            break_type="UNAUTHORIZED_REVERSAL",
            severity="High",
            field="amount_mxn",
            operator="lt",
            value="0",
            filter_type=None,
        ),
        RuleCreate(
            name="Settlement Timeout",
            description="Flag DISPATCHED/PENDING transactions past the 15:00h settlement cutoff (built-in detector)",
            break_type="SETTLEMENT_TIMEOUT",
            severity="High",
            field="timestamp",
            operator="gt",
            value="15:00",
            filter_status="DISPATCHED",
            is_active=True,
        ),
        RuleCreate(
            name="SPEI Duplicate",
            description="Flag SPEI transactions with same base ID — network retry pattern (built-in detector)",
            break_type="SPEI_DUPLICATE",
            severity="Medium",
            field="txn_id",
            operator="contains",
            value="*",
            filter_type="SPEI",
            is_active=True,
        ),
        RuleCreate(
            name="Fee Mismatch",
            description="Flag transactions where applied fee differs from contracted rate",
            break_type="FEE_MISMATCH",
            severity="Medium",
            field="break_flag",
            operator="eq",
            value="FEE_MISMATCH",
            filter_type=None,
        ),
    ]

    created = []
    for rule_data in defaults:
        exists = db.query(BusinessRule).filter(BusinessRule.name == rule_data.name).first()
        if not exists:
            rule = BusinessRule(**rule_data.model_dump())
            db.add(rule)
            created.append(rule_data.name)
    db.commit()
    return {"seeded": created, "skipped": len(defaults) - len(created)}


@router.post("/seed", status_code=201)
def seed_endpoint(db: Session = Depends(get_db), _: None = Depends(require_write_access)):
    return seed_default_rules(db)


@router.post("/reset", status_code=201)
def reset_rules(db: Session = Depends(get_db), _: None = Depends(require_write_access)):
    """Delete all rules and re-seed defaults."""
    db.query(BusinessRule).delete()
    db.commit()
    return seed_default_rules(db)
