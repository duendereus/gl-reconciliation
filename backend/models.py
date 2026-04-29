"""SQLAlchemy models."""

from __future__ import annotations

import secrets
from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, Float, Integer, String, Text, DateTime

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


class User(Base):
    """Simple user for demo auth."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(60), unique=True, nullable=False)
    password = Column(String(120), nullable=False)  # plaintext — demo only
    display_name = Column(String(120), default="")
    role = Column(String(60), default="")
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "username": self.username,
            "display_name": self.display_name,
            "role": self.role,
            "is_admin": bool(self.is_admin),
        }


class Session(Base):
    """Active session token."""

    __tablename__ = "sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=False)
    token = Column(String(64), unique=True, nullable=False, index=True,
                   default=lambda: secrets.token_hex(32))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_seen = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class LoginEvent(Base):
    """Audit log of login attempts."""

    __tablename__ = "login_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(60), nullable=False)
    success = Column(Boolean, nullable=False)
    ip_address = Column(String(45), default="")
    user_agent = Column(String(300), default="")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class PageView(Base):
    """Anonymous page-load tracking (deduplicated by IP per day)."""

    __tablename__ = "page_views"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ip_address = Column(String(45), default="", index=True)
    user_agent = Column(String(300), default="")
    referer = Column(String(300), default="")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class SavedDataset(Base):
    """A dataset upload with persisted analysis results."""

    __tablename__ = "saved_datasets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), nullable=False)
    uploaded_by = Column(Integer, nullable=True)  # user_id
    transaction_count = Column(Integer, default=0)
    break_count = Column(Integer, default=0)
    summary_json = Column(Text, default="{}")
    breaks_json = Column(Text, default="[]")
    analyses_json = Column(Text, default="[]")
    chart_data_json = Column(Text, default="{}")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "transaction_count": self.transaction_count,
            "break_count": self.break_count,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
