import logging
import os
import sys
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
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
        for acc in accounts:
            user = db.query(User).filter(User.username == acc["username"]).first()
            if not user:
                db.add(User(**acc))
                logger.info("Seeded user: %s (admin=%s)", acc["username"], acc["is_admin"])
            else:
                changed = False
                for k, v in acc.items():
                    if getattr(user, k) != v:
                        setattr(user, k, v)
                        changed = True
                if changed:
                    logger.info("Updated user: %s", acc["username"])
        db.commit()
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
async def root():
    """Serve the frontend directly at root."""
    html = os.path.join(os.path.dirname(__file__), "..", "comp_files", "r_reconciliation_v8_consistent.html")
    return FileResponse(html, media_type="text/html")


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": "0.1.0",
        "claude_configured": bool(os.getenv("ANTHROPIC_API_KEY")),
    }
