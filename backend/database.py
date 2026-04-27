"""Database setup — SQLite for local dev, Neon PostgreSQL for production.

Swap by setting DATABASE_URL in .env:
  - Local:  sqlite:///./rules.db
  - Neon:   postgresql://user:pass@host/db
"""

from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

logger = logging.getLogger(__name__)

_SQLITE_DEFAULT = "sqlite:///./rules.db"
_raw_url = os.getenv("DATABASE_URL", "")

# Fall back to SQLite if DATABASE_URL is empty or a placeholder
if not _raw_url or "user:password@host" in _raw_url:
    DATABASE_URL = _SQLITE_DEFAULT
else:
    DATABASE_URL = _raw_url

# Neon/Railway may provide postgres:// — SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# SQLite needs check_same_thread=False for FastAPI
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def get_db():
    """FastAPI dependency that yields a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables and apply lightweight migrations.

    SQLAlchemy's create_all only creates missing tables — it does NOT add
    missing columns to existing tables. We patch that by inspecting each
    declared model and ALTER-ing in any columns the live DB is missing.
    """
    # Ensure all model classes are imported so metadata is populated
    import backend.models  # noqa: F401
    Base.metadata.create_all(bind=engine)
    _apply_column_migrations()


def _apply_column_migrations():
    """Detect and add missing columns on existing tables (idempotent)."""
    insp = inspect(engine)
    is_pg = engine.url.get_backend_name() == "postgresql"

    for table in Base.metadata.tables.values():
        if not insp.has_table(table.name):
            continue  # create_all just made it; nothing to migrate
        existing_cols = {c["name"] for c in insp.get_columns(table.name)}
        for col in table.columns:
            if col.name in existing_cols:
                continue
            col_type = col.type.compile(dialect=engine.dialect)
            default_clause = ""
            if col.default is not None and getattr(col.default, "is_scalar", False):
                val = col.default.arg
                if isinstance(val, bool):
                    default_clause = f" DEFAULT {'TRUE' if val else 'FALSE'}"
                elif isinstance(val, (int, float)):
                    default_clause = f" DEFAULT {val}"
                elif isinstance(val, str):
                    default_clause = f" DEFAULT '{val}'"
            null_clause = "" if col.nullable else " NOT NULL"
            sql = f'ALTER TABLE {table.name} ADD COLUMN {col.name} {col_type}{default_clause}{null_clause}'
            try:
                with engine.begin() as conn:
                    conn.execute(text(sql))
                logger.info("Migrated: added column %s.%s", table.name, col.name)
            except Exception as exc:
                logger.warning("Migration skipped for %s.%s: %s", table.name, col.name, exc)
