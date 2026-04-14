"""SQLAlchemy models."""

from __future__ import annotations

from sqlalchemy import Boolean, Column, Float, Integer, String, Text, DateTime
from datetime import datetime, timezone

from backend.database import Base


class BusinessRule(Base):
    """A configurable reconciliation rule.

    Each rule defines a detection condition. The reconciliation engine
    evaluates active rules against transaction data to find breaks.

    Attributes
    ----------
    field : str
        The CSV column to evaluate (e.g. ``rate_applied``, ``amount_mxn``).
    operator : str
        Comparison operator: ``gt``, ``lt``, ``eq``, ``neq``, ``delta_gt``,
        ``contains``, ``not_contains``.
    value : str
        Threshold or reference value (stored as string, cast at runtime).
    compare_field : str | None
        Optional second column for field-vs-field comparisons (e.g.
        ``rate_applied`` delta vs ``rate_reference``).
    filter_type : str | None
        Restrict the rule to a specific transaction type (e.g.
        ``SAVINGS_ACCOUNT``). ``None`` = apply to all types.
    filter_status : str | None
        Restrict to a specific status (e.g. ``DISPATCHED``).
    """

    __tablename__ = "business_rules"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(120), nullable=False)
    description = Column(Text, default="")
    break_type = Column(String(60), nullable=False)       # e.g. FX_RATE, CUSTOM
    severity = Column(String(20), default="Medium")       # Low | Medium | High | Critical
    field = Column(String(60), nullable=False)             # CSV column
    operator = Column(String(20), nullable=False)          # gt, lt, eq, neq, delta_gt, contains, not_contains
    value = Column(String(120), nullable=False)            # threshold
    compare_field = Column(String(60), nullable=True)      # optional second column
    filter_type = Column(String(60), nullable=True)        # restrict to txn type
    filter_status = Column(String(30), nullable=True)      # restrict to status
    is_active = Column(Boolean, default=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "break_type": self.break_type,
            "severity": self.severity,
            "field": self.field,
            "operator": self.operator,
            "value": self.value,
            "compare_field": self.compare_field,
            "filter_type": self.filter_type,
            "filter_status": self.filter_status,
            "is_active": self.is_active,
        }


class AnalysisCache(Base):
    """Cached Claude analysis result for a break.

    Keyed by a hash of (txn_id, break_type, impact_mxn) so re-analyzing
    the same break returns the cached result without calling Claude again.
    """

    __tablename__ = "analysis_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cache_key = Column(String(64), unique=True, nullable=False, index=True)
    txn_id = Column(String(30), nullable=False)
    analysis_json = Column(Text, nullable=False)  # full BreakAnalysis as JSON
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
