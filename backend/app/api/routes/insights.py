"""
insights.py
-----------
API routes for Step 3: Findings generation and AI-powered insights.

Endpoints
---------
POST /datasets/{dataset_id}/insights/generate?month=YYYY-MM
    Build deterministic Findings from KPI tables, check the LLM cache,
    call the AI service if needed, and return a structured AnalysisResponse.

GET /datasets/{dataset_id}/insights/latest
    Return the most recently generated insight for a dataset.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.db import models
from app.db.session import get_db
from app.schemas.insights import InsightGenerateResponse
from app.services import ai_insights, insight_engine
from app.services.ai_insights import AIServiceUnavailableError

logger = logging.getLogger(__name__)

router = APIRouter()

# Increment this string whenever the system prompt in ai_insights.py changes.
# Doing so invalidates all existing llm_cache entries for the new prompt
# without touching the database (different hash → cache miss → fresh LLM call).
_PROMPT_VERSION = "insights_v1"


# ---------------------------------------------------------------------------
# POST /datasets/{dataset_id}/insights/generate
# ---------------------------------------------------------------------------


@router.post(
    "/{dataset_id}/insights/generate",
    response_model=InsightGenerateResponse,
    status_code=status.HTTP_200_OK,
    summary="Generate AI insights for a dataset month (cached)",
)
def generate_insights(
    dataset_id: str,
    month: str = Query(
        ...,
        description="Target month in YYYY-MM format (e.g. 2024-03).",
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
    ),
    db: Session = Depends(get_db),
) -> InsightGenerateResponse:
    """
    Build a compact Findings payload from pre-computed KPI tables, then
    generate (or serve from cache) a structured AI AnalysisResponse.

    Flow
    ----
    1. Verify dataset exists → 404 if not.
    2. Parse month → date object (first of month).
    3. Build deterministic Findings from KPI tables → 422 if KPIs not run yet.
    4. Compute SHA-256 digest_hash(Findings + prompt_version).
    5. Check llm_cache: if hit → return cached response (cached=True).
    6. Cache miss → call AI service → store result in llm_cache.
    7. Upsert into insights table (one row per dataset+month, for audit).
    8. Return InsightGenerateResponse.

    The LLM never sees raw revenue_events rows — only the compact Findings.
    Identical Findings always produce the same hash and hit the cache.

    Raises
    ------
    404  Dataset not found.
    422  KPI data not available for the requested month (run /kpis/run first).
    500  AI service error after retry.
    """
    # --- 1. Verify dataset ---
    dataset = db.query(models.Dataset).filter_by(id=dataset_id).first()
    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset '{dataset_id}' not found.",
        )

    # --- 2. Parse month ---
    try:
        year_str, mon_str = month.split("-")
        month_date = date(int(year_str), int(mon_str), 1)
    except (ValueError, AttributeError):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="month must be in YYYY-MM format (e.g. 2024-03).",
        )

    # --- 3. Build Findings ---
    try:
        findings = insight_engine.build_findings(dataset_id, month_date, db)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )

    # --- 4. Digest hash ---
    digest_hash = insight_engine.compute_digest_hash(findings, _PROMPT_VERSION)

    # --- 5. Cache lookup ---
    cached_entry = (
        db.query(models.LlmCache).filter_by(digest_hash=digest_hash).first()
    )

    if cached_entry:
        try:
            from app.schemas.analysis_response import AnalysisResponse
            analysis = AnalysisResponse.model_validate_json(cached_entry.response_json)
            cached = True
        except (json.JSONDecodeError, ValidationError) as exc:
            # Corrupt cache entry — evict and regenerate
            logger.warning("Corrupt LLM cache entry for %s; evicting. %s", digest_hash, exc)
            db.delete(cached_entry)
            db.flush()
            cached_entry = None

    if not cached_entry:
        # --- 6. Generate via AI ---
        try:
            analysis = ai_insights.generate_insights_from_findings(findings)
        except AIServiceUnavailableError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"AI provider temporarily unavailable: {exc}",
            )
        except (ValidationError, Exception) as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"AI insight generation failed: {exc}",
            )

        # --- 6b. Store in LLM cache ---
        db.add(
            models.LlmCache(
                digest_hash=digest_hash,
                response_json=analysis.model_dump_json(),
                created_at=datetime.now(timezone.utc),
            )
        )
        cached = False

    # --- 7. Overwrite latest snapshot (one row per dataset+month) ---
    db.query(models.Insight).filter_by(
        dataset_id=dataset_id, month=month_date
    ).delete(synchronize_session=False)

    db.add(
        models.Insight(
            dataset_id=dataset_id,
            month=month_date,
            digest_hash=digest_hash,
            prompt_version=_PROMPT_VERSION,
            findings_json=findings.model_dump_json(),
            response_json=analysis.model_dump_json(),
            created_at=datetime.now(timezone.utc),
        )
    )

    db.commit()

    # --- 8. Return ---
    return InsightGenerateResponse(
        dataset_id=dataset_id,
        month=month,
        digest_hash=digest_hash,
        cached=cached,
        analysis=analysis,
    )


# ---------------------------------------------------------------------------
# GET /datasets/{dataset_id}/insights/latest
# ---------------------------------------------------------------------------


@router.get(
    "/{dataset_id}/insights/latest",
    response_model=InsightGenerateResponse,
    summary="Get the latest saved insight for a dataset",
)
def get_latest_insight(
    dataset_id: str,
    db: Session = Depends(get_db),
) -> InsightGenerateResponse:
    """
    Return the most recently generated insight for the given dataset.

    Useful for displaying the last AI analysis without triggering a new
    generation. Always sets cached=True since the result comes from the
    insights audit table.

    Raises
    ------
    404  Dataset not found, or no insights generated yet.
    """
    dataset = db.query(models.Dataset).filter_by(id=dataset_id).first()
    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset '{dataset_id}' not found.",
        )

    row = (
        db.query(models.Insight)
        .filter_by(dataset_id=dataset_id)
        .order_by(models.Insight.created_at.desc())
        .first()
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No insights found for dataset '{dataset_id}'. "
                "Generate insights first via POST /datasets/{id}/insights/generate."
            ),
        )

    from app.schemas.analysis_response import AnalysisResponse
    analysis = AnalysisResponse.model_validate_json(row.response_json)

    return InsightGenerateResponse(
        dataset_id=dataset_id,
        month=row.month.strftime("%Y-%m"),
        digest_hash=row.digest_hash,
        cached=True,
        analysis=analysis,
    )
