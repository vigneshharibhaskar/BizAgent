from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------
# connect_args is SQLite-specific: allows the same connection to be used
# across multiple threads (FastAPI runs handlers in a thread pool).
# Remove connect_args entirely when switching to PostgreSQL.
engine = create_engine(
    settings.DATABASE_URL,
    connect_args={"check_same_thread": False},  # SQLite only
    echo=False,  # Set True to log all SQL statements during development
)

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------
def get_db() -> Generator[Session, None, None]:
    """
    Yield a SQLAlchemy Session and guarantee it is closed after the request,
    regardless of whether the handler raises an exception.

    Usage in a route:
        def my_route(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Table initialisation
# ---------------------------------------------------------------------------
def create_all_tables() -> None:
    """
    Create all tables defined via SQLAlchemy's declarative base if they do
    not already exist. Called once from main.py's lifespan handler.

    Uses a local import of Base to avoid a circular import chain:
    models -> Base -> session -> models.
    """
    from app.db.models import Base  # local import prevents circular dependency

    Base.metadata.create_all(bind=engine)
