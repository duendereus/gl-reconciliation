import logging
import os
import sys
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.database import init_db
from backend.routes.admin import router as admin_router
from backend.routes.analyze import router as analyze_router
from backend.routes.auth import router as auth_router
from backend.routes.datasets import router as datasets_router
from backend.routes.rules import router as rules_router

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("r-reconciliation")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Validate env
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    db_url = os.getenv("DATABASE_URL", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — Claude analysis will use fallback mode")
    if db_url:
        logger.info("DATABASE_URL configured (using external DB)")
    else:
        logger.info("DATABASE_URL not set — using local SQLite (rules.db)")

    init_db()
    logger.info("Database initialized")

    # Seed/update demo + admin users on startup
    from backend.database import SessionLocal
    from backend.models import User

    accounts = [
        {
            "username": os.getenv("DEMO_USERNAME", "demo"),
            "password": os.getenv("DEMO_PASSWORD", "demo2026"),
            "display_name": os.getenv("DEMO_DISPLAY_NAME", "Demo User"),
            "role": os.getenv("DEMO_ROLE", "Finance Operations · Demo Account"),
            "is_admin": False,
        },
        {
            "username": os.getenv("ADMIN_USERNAME", "admin"),
            "password": os.getenv("ADMIN_PASSWORD", "changeme"),
            "display_name": os.getenv("ADMIN_DISPLAY_NAME", "Admin"),
            "role": os.getenv("ADMIN_ROLE", "Owner"),
            "is_admin": True,
        },
    ]

    db = SessionLocal()
    try:
        configured_usernames = {acc["username"] for acc in accounts}
        for acc in accounts:
            # Mask password in logs (first 2 chars + length)
            pw_mask = f"{acc['password'][:2]}***({len(acc['password'])} chars)"
            using_default = acc["password"] == "changeme" or acc["password"] == "demo2026"
            warn = " ⚠ DEFAULT — INSECURE" if using_default and acc.get("is_admin") else ""
            logger.info("User config: %s · admin=%s · password=%s%s",
                        acc["username"], acc["is_admin"], pw_mask, warn)
            try:
                user = db.query(User).filter(User.username == acc["username"]).first()
                if not user:
                    db.add(User(**acc))
                    logger.info("  → Seeded NEW user: %s", acc["username"])
                else:
                    changes = []
                    for k, v in acc.items():
                        if getattr(user, k, None) != v:
                            if k == "password":
                                changes.append("password (rotated)")
                            else:
                                changes.append(f"{k}")
                            setattr(user, k, v)
                    if changes:
                        logger.info("  → Updated existing user %s: %s",
                                    acc["username"], ", ".join(changes))
                    else:
                        logger.info("  → User %s already up-to-date", acc["username"])
                db.commit()
            except Exception as exc:
                logger.warning("Could not seed user %s: %s", acc["username"], exc)
                db.rollback()

        # Cleanup: remove any orphan admin users that aren't in the current config.
        # Prevents leftover admin accounts (e.g. default 'admin/changeme') from
        # surviving when ADMIN_USERNAME is rotated.
        try:
            orphan_admins = (
                db.query(User)
                .filter(User.is_admin == True, User.username.notin_(configured_usernames))
                .all()
            )
            for u in orphan_admins:
                logger.warning("Removing orphan admin user: %s (was seeded with old config)", u.username)
                db.delete(u)
            if orphan_admins:
                db.commit()
        except Exception as exc:
            logger.warning("Orphan admin cleanup failed: %s", exc)
            db.rollback()
    finally:
        db.close()

    port = os.getenv("PORT", "8000")
    logger.info("R. GL Reconciliation API ready on port %s", port)
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="R. GL Reconciliation API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(datasets_router, prefix="/datasets", tags=["datasets"])
app.include_router(analyze_router, prefix="/analyze", tags=["analyze"])
app.include_router(rules_router, prefix="/rules", tags=["rules"])
app.include_router(admin_router, prefix="/admin", tags=["admin"])

# Serve frontend static files
comp_dir = os.path.join(os.path.dirname(__file__), "..", "comp_files")
if os.path.isdir(comp_dir):
    app.mount("/static", StaticFiles(directory=comp_dir), name="static")


@app.get("/")
async def root(request: Request):
    """Serve the frontend directly at root + log a page view."""
    try:
        from backend.database import SessionLocal
        from backend.models import PageView
        db = SessionLocal()
        try:
            db.add(PageView(
                ip_address=request.client.host if request.client else "",
                user_agent=request.headers.get("user-agent", "")[:300],
                referer=request.headers.get("referer", "")[:300],
            ))
            db.commit()
        finally:
            db.close()
    except Exception as exc:
        logger.warning("Page view logging failed: %s", exc)
    html = os.path.join(os.path.dirname(__file__), "..", "comp_files", "r_reconciliation_v8_consistent.html")
    return FileResponse(html, media_type="text/html")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "0.1.0",
        "claude_configured": bool(os.getenv("ANTHROPIC_API_KEY")),
    }
