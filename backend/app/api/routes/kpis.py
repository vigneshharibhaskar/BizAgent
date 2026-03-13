import json

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.db import models
from app.db.session import get_db
from app.schemas.kpis import (
    ChurnMonthlyResponse,
    CohortRetentionResponse,
    KpiRunResponse,
    MrrMonthlyResponse,
    SegmentMonthlyResponse,
)
from app.services import kpi_engine

router = APIRouter()


# ---------------------------------------------------------------------------
# POST /datasets/{dataset_id}/kpis/run
# ---------------------------------------------------------------------------


@router.post(
    "/{dataset_id}/kpis/run",
    response_model=KpiRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Compute and store all KPIs for a dataset",
)
def run_kpis(
    dataset_id: str,
    db: Session = Depends(get_db),
) -> KpiRunResponse:
    """
    Trigger full KPI computation for the given dataset.

    Computes MRR components, churn metrics, segment breakdowns, and cohort
    retention curves from the raw revenue_events data. Results are stored in
    the four KPI tables, replacing any previously computed values.

    This endpoint is idempotent — running it multiple times on the same
    dataset always produces the same result (results are fully replaced).

    Raises
    ------
    404  if the dataset does not exist.
    422  if the dataset has no revenue events.
    500  for unexpected computation errors.
    """
    # Verify the dataset exists before doing any computation
    dataset = db.query(models.Dataset).filter(models.Dataset.id == dataset_id).first()
    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset '{dataset_id}' not found.",
        )

    try:
        summary = kpi_engine.run_kpis(dataset_id=dataset_id, db=db)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"KPI computation failed: {exc}",
        )

    warnings = summary.get("warnings", [])

    # Persist warnings on the dataset row so insight_engine can include them
    # in Findings without a separate KPI-run-log table.
    # kpi_engine.run_kpis() already called db.commit(), so this is a second,
    # lightweight commit for just the metadata update.
    dataset.latest_kpi_warnings = json.dumps(warnings)
    db.commit()

    return KpiRunResponse(
        dataset_id=dataset_id,
        months_computed=summary["months_computed"],
        segments_computed=summary["segments_computed"],
        cohorts_computed=summary["cohorts_computed"],
        warnings=warnings,
        message=(
            f"KPIs computed for dataset '{dataset.name}': "
            f"{summary['months_computed']} months, "
            f"{summary['segments_computed']} segment rows, "
            f"{summary['cohorts_computed']} cohort data points."
            + (f" {len(warnings)} warning(s)." if warnings else "")
        ),
    )


# ---------------------------------------------------------------------------
# GET /datasets/{dataset_id}/kpis/mrr
# ---------------------------------------------------------------------------


@router.get(
    "/{dataset_id}/kpis/mrr",
    response_model=list[MrrMonthlyResponse],
    summary="Get monthly MRR components for a dataset",
)
def get_mrr(
    dataset_id: str,
    db: Session = Depends(get_db),
) -> list[MrrMonthlyResponse]:
    """
    Return pre-computed monthly MRR and its growth components.

    Run POST /{dataset_id}/kpis/run first to populate these results.
    Returns an empty list if KPIs have not been computed yet.
    """
    rows = (
        db.query(models.KpiMrrMonthly)
        .filter_by(dataset_id=dataset_id)
        .order_by(models.KpiMrrMonthly.month)
        .all()
    )
    return rows


# ---------------------------------------------------------------------------
# GET /datasets/{dataset_id}/kpis/churn
# ---------------------------------------------------------------------------


@router.get(
    "/{dataset_id}/kpis/churn",
    response_model=list[ChurnMonthlyResponse],
    summary="Get monthly churn, GRR, and NRR for a dataset",
)
def get_churn(
    dataset_id: str,
    db: Session = Depends(get_db),
) -> list[ChurnMonthlyResponse]:
    """Return pre-computed monthly churn rates, GRR, and NRR."""
    rows = (
        db.query(models.KpiChurnMonthly)
        .filter_by(dataset_id=dataset_id)
        .order_by(models.KpiChurnMonthly.month)
        .all()
    )
    return rows


# ---------------------------------------------------------------------------
# GET /datasets/{dataset_id}/kpis/segments
# ---------------------------------------------------------------------------


@router.get(
    "/{dataset_id}/kpis/segments",
    response_model=list[SegmentMonthlyResponse],
    summary="Get monthly segment MRR and churn for a dataset",
)
def get_segments(
    dataset_id: str,
    segment_type: str | None = None,
    db: Session = Depends(get_db),
) -> list[SegmentMonthlyResponse]:
    """
    Return pre-computed segment metrics.

    Optional query parameter:
        segment_type : Filter to a single dimension ('plan', 'region', 'channel').
    """
    query = db.query(models.KpiSegmentsMonthly).filter_by(dataset_id=dataset_id)
    if segment_type:
        query = query.filter(models.KpiSegmentsMonthly.segment_type == segment_type)
    rows = query.order_by(
        models.KpiSegmentsMonthly.month,
        models.KpiSegmentsMonthly.segment_type,
        models.KpiSegmentsMonthly.segment_value,
    ).all()
    return rows


# ---------------------------------------------------------------------------
# GET /datasets/{dataset_id}/kpis/cohorts
# ---------------------------------------------------------------------------


@router.get(
    "/{dataset_id}/kpis/cohorts",
    response_model=list[CohortRetentionResponse],
    summary="Get cohort retention curves for a dataset",
)
def get_cohorts(
    dataset_id: str,
    db: Session = Depends(get_db),
) -> list[CohortRetentionResponse]:
    """Return pre-computed cohort retention data points."""
    rows = (
        db.query(models.CohortRetention)
        .filter_by(dataset_id=dataset_id)
        .order_by(
            models.CohortRetention.cohort_month,
            models.CohortRetention.age_month,
        )
        .all()
    )
    return rows
