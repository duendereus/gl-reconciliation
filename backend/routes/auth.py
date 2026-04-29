"""Auth endpoints — simple token-based session for the demo."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session as DBSession

from backend.database import get_db
from backend.models import LoginEvent, Session, User

router = APIRouter()


def is_read_only_mode() -> bool:
    """Whether the deployment is in read-only mode (set via READ_ONLY env var)."""
    return os.getenv("READ_ONLY", "false").lower() in ("1", "true", "yes")


def require_write_access(
    authorization: str | None = Header(None),
    db: DBSession = Depends(get_db),
):
    """FastAPI dependency: allow only admin users when READ_ONLY is set."""
    if not is_read_only_mode():
        return  # writes open to all
    # Read-only is on — only admins pass
    if not authorization:
        raise HTTPException(status_code=403, detail="Read-only demo · sign in as admin to make changes.")
    token = authorization.replace("Bearer ", "")
    session = db.query(Session).filter(Session.token == token).first()
    if not session:
        raise HTTPException(status_code=403, detail="Read-only demo · sign in as admin to make changes.")
    user = db.query(User).filter(User.id == session.user_id).first()
    if not user or not user.is_admin:
        raise HTTPException(status_code=403, detail="Read-only demo · this action requires admin access.")


class LoginRequest(BaseModel):
    username: str
    password: str


def get_current_user(
    authorization: str | None = Header(None),
    db: DBSession = Depends(get_db),
) -> User:
    """FastAPI dependency: validate token and return user."""
    if not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")
    token = authorization.replace("Bearer ", "")
    session = db.query(Session).filter(Session.token == token).first()
    if not session:
        raise HTTPException(status_code=401, detail="Invalid session")
    user = db.query(User).filter(User.id == session.user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    # Update last_seen
    session.last_seen = datetime.now(timezone.utc)
    db.commit()
    return user


@router.post("/login")
def login(data: LoginRequest, request: Request, db: DBSession = Depends(get_db)):
    ip = request.client.host if request.client else ""
    ua = request.headers.get("user-agent", "")[:300]

    user = db.query(User).filter(User.username == data.username).first()
    if not user or user.password != data.password:
        db.add(LoginEvent(username=data.username, success=False, ip_address=ip, user_agent=ua))
        db.commit()
        raise HTTPException(status_code=401, detail="Invalid credentials")

    # Create session
    session = Session(user_id=user.id)
    db.add(session)
    db.add(LoginEvent(username=data.username, success=True, ip_address=ip, user_agent=ua))
    db.commit()
    db.refresh(session)

    return {
        "token": session.token,
        "user": user.to_dict(),
    }


@router.get("/me")
def get_me(user: User = Depends(get_current_user)):
    return {"user": user.to_dict()}


@router.post("/logout")
def logout(
    authorization: str | None = Header(None),
    db: DBSession = Depends(get_db),
):
    if authorization:
        token = authorization.replace("Bearer ", "")
        session = db.query(Session).filter(Session.token == token).first()
        if session:
            db.delete(session)
            db.commit()
    return {"status": "ok"}


@router.get("/config")
def get_config():
    """Public config the frontend reads at boot (no auth)."""
    return {"read_only": is_read_only_mode()}


def require_admin(
    authorization: str | None = Header(None),
    db: DBSession = Depends(get_db),
) -> User:
    """FastAPI dependency: only admin users may pass."""
    if not authorization:
        raise HTTPException(status_code=403, detail="Admin only")
    token = authorization.replace("Bearer ", "")
    session = db.query(Session).filter(Session.token == token).first()
    if not session:
        raise HTTPException(status_code=403, detail="Admin only")
    user = db.query(User).filter(User.id == session.user_id).first()
    if not user or not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin only")
    return user


@router.get("/login-events")
def list_login_events(
    db: DBSession = Depends(get_db),
    _: User = Depends(require_admin),
):
    events = (
        db.query(LoginEvent).order_by(LoginEvent.created_at.desc()).limit(200).all()
    )
    # Aggregates
    successful = [e for e in events if e.success]
    unique_ips = {e.ip_address for e in successful if e.ip_address}
    unique_users = {e.username for e in successful}
    return {
        "stats": {
            "total_attempts": len(events),
            "successful_logins": len(successful),
            "failed_attempts": len(events) - len(successful),
            "unique_ips": len(unique_ips),
            "unique_users_logged_in": len(unique_users),
            "ips": sorted(unique_ips),
            "users": sorted(unique_users),
        },
        "events": [
            {
                "id": e.id,
                "username": e.username,
                "success": e.success,
                "ip_address": e.ip_address,
                "user_agent": (e.user_agent or "")[:120],
                "created_at": e.created_at.isoformat() if e.created_at else None,
            }
            for e in events
        ],
    }
