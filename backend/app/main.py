from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import ask, insights, kpis, upload
from app.core.config import settings
from app.db.session import create_all_tables


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager: code before 'yield' runs on startup,
    code after 'yield' runs on shutdown.

    On startup: initialise the SQLite database by creating all tables
    defined in app.db.models if they do not already exist.
    """
    create_all_tables()
    yield
    # Shutdown logic (connection pool cleanup, etc.) can go here.


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="AI Business Analyst Agent backend for SaaS revenue analytics.",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS — permissive for local dev; tighten in production via environment vars
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(upload.router, prefix="/datasets", tags=["datasets"])
app.include_router(kpis.router, prefix="/datasets", tags=["kpis"])
app.include_router(insights.router, prefix="/datasets", tags=["insights"])
app.include_router(ask.router, prefix="/datasets", tags=["ask"])


# ---------------------------------------------------------------------------
# Health check — outside any router prefix so infra probes work independently
# ---------------------------------------------------------------------------
@app.get("/health", tags=["health"])
def health_check():
    """Liveness probe. Returns application status and current version."""
    return {"status": "ok", "version": settings.APP_VERSION}
