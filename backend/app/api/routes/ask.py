"""
ask.py
------
API route for Step 5: the interactive Ask Agent.

Endpoints
---------
POST /datasets/{dataset_id}/ask
    Accept a natural-language query, run the LangGraph Ask Agent, and return
    a structured AnalysisResponse. Supports optional debug tracing.
"""

from __future__ import annotations

import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.agent.ask_graph import run_ask
from app.db import models
from app.db.session import get_db
from app.schemas.ask import AskRequest, AskResponse
from app.services.ai_insights import AIServiceUnavailableError

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# POST /datasets/{dataset_id}/ask
# ---------------------------------------------------------------------------


@router.post(
    "/{dataset_id}/ask",
    response_model=AskResponse,
    status_code=status.HTTP_200_OK,
    summary="Ask the AI agent a question about a dataset",
)
def ask_agent(
    dataset_id: str,
    body: AskRequest,
    db: Session = Depends(get_db),
) -> AskResponse:
    """
    Run the LangGraph Ask Agent for a natural-language query.

    Flow
    ----
    1. Verify dataset exists → 404 if not.
    2. Resolve target month:
       - Use body.month if provided.
       - Otherwise pick the latest month with KPI data in kpi_mrr_monthly.
       - 422 if no KPI data exists (run POST /kpis/run first).
    3. Run the Ask Agent (LangGraph graph with ≤ 2 tool loops).
    4. Return AskResponse with AnalysisResponse + optional AgentTrace.

    The LLM never sees raw revenue_events rows — only compact KPI aggregates
    from the deterministic ask_tools functions.

    Raises
    ------
    404  Dataset not found.
    422  Month not provided and no KPI data exists for the dataset.
    422  Provided month has no KPI data (run /kpis/run first).
    503  AI provider temporarily unavailable (transient network/rate-limit error).
    500  Both LLM attempts returned an invalid schema.
    """
    # --- 1. Verify dataset ---
    dataset = db.query(models.Dataset).filter_by(id=dataset_id).first()
    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset '{dataset_id}' not found.",
        )

    # --- 2. Resolve month ---
    month_date = _resolve_month(dataset_id, body.month, db)

    # --- 3. Run agent ---
    try:
        analysis, trace = run_ask(
            dataset_id=dataset_id,
            query=body.query,
            month_date=month_date,
            db=db,
            debug=body.debug,
        )
    except AIServiceUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"AI provider temporarily unavailable: {exc}",
        )
    except (ValidationError, Exception) as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ask Agent failed: {exc}",
        )

    # --- 4. Return ---
    return AskResponse(
        dataset_id=dataset_id,
        query=body.query,
        month=month_date.strftime("%Y-%m"),
        analysis=analysis,
        trace=trace if body.debug else None,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _resolve_month(dataset_id: str, month_str: str | None, db: Session) -> date:
    """
    Return the target month as a date object (first day of month).

    If month_str is provided, parse and validate it. Otherwise pick the most
    recent month that has KPI data in kpi_mrr_monthly.

    Raises
    ------
    HTTPException 422 : No KPI data exists, or provided month has no data.
    """
    if month_str:
        try:
            year_str, mon_str = month_str.split("-")
            month_date = date(int(year_str), int(mon_str), 1)
        except (ValueError, AttributeError):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="month must be in YYYY-MM format (e.g. 2024-03).",
            )
        # Verify KPI data exists for this month
        exists = (
            db.query(models.KpiMrrMonthly)
            .filter_by(dataset_id=dataset_id, month=month_date)
            .first()
        )
        if not exists:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    f"No KPI data for dataset '{dataset_id}' in {month_str}. "
                    "Run POST /datasets/{dataset_id}/kpis/run first."
                ),
            )
        return month_date

    # Auto-detect latest month
    row = (
        db.query(models.KpiMrrMonthly.month)
        .filter(models.KpiMrrMonthly.dataset_id == dataset_id)
        .order_by(models.KpiMrrMonthly.month.desc())
        .first()
    )
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"No KPI data found for dataset '{dataset_id}'. "
                "Run POST /datasets/{dataset_id}/kpis/run first."
            ),
        )
    return row[0]
