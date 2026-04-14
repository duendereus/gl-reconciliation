"""Database setup — SQLite for local dev, Neon PostgreSQL for production.

Swap by setting DATABASE_URL in .env:
  - Local:  sqlite:///./rules.db
  - Neon:   postgresql://user:pass@host/db
"""

from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

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
    """Create all tables. Called at app startup."""
    Base.metadata.create_all(bind=engine)
