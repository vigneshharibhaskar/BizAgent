"""
insight_engine.py
-----------------
Deterministic Findings engine for Step 3 of BizAgent.

Responsibilities
----------------
- Read pre-computed KPI aggregates from the four KPI tables.
- Build a compact, LLM-safe Findings payload (no raw event rows).
- Compute a stable SHA-256 digest_hash over (Findings JSON, prompt_version)
  for LLM response caching.

This module has NO knowledge of HTTP and makes NO LLM calls.
It only reads from the database; it never writes.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

_FINDINGS_SIZE_WARN_BYTES = 8_192  # warn if payload exceeds 8 KB

from sqlalchemy.orm import Session

from app.db import models
from app.schemas.findings import (
    CohortPoint,
    Cohorts,
    DataQuality,
    Drivers,
    Findings,
    HeadlineMetrics,
    MovementSummary,
    SegmentRow,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_findings(dataset_id: str, month: date, db: Session) -> Findings:
    """
    Build a deterministic Findings payload from KPI tables for one month.

    Queries kpi_mrr_monthly, kpi_churn_monthly, kpi_segments_monthly, and
    cohort_retention. Raw revenue_events are never touched.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset.
    month      : First day of the target month (e.g. date(2024, 3, 1)).
    db         : Active SQLAlchemy Session.

    Returns
    -------
    Findings payload ready for hashing and LLM consumption.

    Raises
    ------
    ValueError : If no KPI data exists for (dataset_id, month). The caller
                 should instruct the user to run POST /kpis/run first.
    """
    prev_month = _prev_month(month)

    # ------------------------------------------------------------------
    # MRR headline
    # ------------------------------------------------------------------
    mrr_curr = (
        db.query(models.KpiMrrMonthly)
        .filter_by(dataset_id=dataset_id, month=month)
        .first()
    )
    if mrr_curr is None:
        raise ValueError(
            f"No KPI data for dataset '{dataset_id}' in "
            f"{month.strftime('%Y-%m')}. "
            "Run POST /datasets/{dataset_id}/kpis/run first."
        )
    mrr_prev_row = (
        db.query(models.KpiMrrMonthly)
        .filter_by(dataset_id=dataset_id, month=prev_month)
        .first()
    )

    # ------------------------------------------------------------------
    # Churn headline
    # ------------------------------------------------------------------
    churn_curr = (
        db.query(models.KpiChurnMonthly)
        .filter_by(dataset_id=dataset_id, month=month)
        .first()
    )
    churn_prev = (
        db.query(models.KpiChurnMonthly)
        .filter_by(dataset_id=dataset_id, month=prev_month)
        .first()
    )

    # ------------------------------------------------------------------
    # Headline metrics
    # ------------------------------------------------------------------
    mrr_val = mrr_curr.mrr
    mrr_prev_val = mrr_prev_row.mrr if mrr_prev_row else None

    headline = HeadlineMetrics(
        month=month.strftime("%Y-%m"),
        mrr=mrr_val,
        mrr_prev=mrr_prev_val,
        mrr_delta_pct=_pct_delta(mrr_val, mrr_prev_val),
        new_mrr=mrr_curr.new_mrr,
        expansion_mrr=mrr_curr.expansion_mrr,
        contraction_mrr=mrr_curr.contraction_mrr,
        churn_mrr=mrr_curr.churn_mrr,
        net_new_mrr=mrr_curr.net_new_mrr,
        customer_churn_rate=churn_curr.customer_churn_rate if churn_curr else None,
        customer_churn_rate_prev=(
            churn_prev.customer_churn_rate if churn_prev else None
        ),
        customer_churn_delta_pp=_pp_delta(
            churn_curr.customer_churn_rate if churn_curr else None,
            churn_prev.customer_churn_rate if churn_prev else None,
        ),
        revenue_churn_rate=churn_curr.revenue_churn_rate if churn_curr else None,
        grr=churn_curr.grr if churn_curr else None,
        grr_prev=churn_prev.grr if churn_prev else None,
        nrr=churn_curr.nrr if churn_curr else None,
        nrr_prev=churn_prev.nrr if churn_prev else None,
    )

    # ------------------------------------------------------------------
    # Segment data — current and previous month
    # ------------------------------------------------------------------
    segs_curr = (
        db.query(models.KpiSegmentsMonthly)
        .filter_by(dataset_id=dataset_id, month=month)
        .all()
    )
    segs_prev = (
        db.query(models.KpiSegmentsMonthly)
        .filter_by(dataset_id=dataset_id, month=prev_month)
        .all()
    )

    seg_rows = _build_segment_rows(segs_curr, segs_prev)

    # Movement summary: top 3 positive / negative by MRR % change
    with_delta = [r for r in seg_rows if r.mrr_delta_pct is not None]
    top_positive = sorted(
        [r for r in with_delta if r.mrr_delta_pct > 0],
        key=lambda r: r.mrr_delta_pct,  # type: ignore[return-value]
        reverse=True,
    )[:3]
    top_negative = sorted(
        [r for r in with_delta if r.mrr_delta_pct < 0],
        key=lambda r: r.mrr_delta_pct,  # type: ignore[return-value]
    )[:3]

    movement_summary = MovementSummary(
        top_positive=top_positive,
        top_negative=top_negative,
    )

    # Drivers: highest MRR-at-risk and biggest MRR declines
    churn_segments = sorted(
        [r for r in seg_rows if (r.mrr_at_risk or 0.0) > 0],
        key=lambda r: r.mrr_at_risk or 0.0,
        reverse=True,
    )[:5]
    mrr_decline_segments = sorted(
        [r for r in with_delta if r.mrr_delta_pct < 0],
        key=lambda r: r.mrr_delta_pct,  # type: ignore[return-value]
    )[:5]

    drivers = Drivers(
        churn_segments=churn_segments,
        mrr_decline_segments=mrr_decline_segments,
    )

    # ------------------------------------------------------------------
    # Cohorts — 2 latest cohorts at ages 0, 1, 3
    # ------------------------------------------------------------------
    cohorts = Cohorts(points=_build_cohort_points(dataset_id, db))

    # ------------------------------------------------------------------
    # Data quality warnings from most recent KPI run
    # ------------------------------------------------------------------
    warnings_list: list[str] = []
    dataset_row = db.query(models.Dataset).filter_by(id=dataset_id).first()
    if dataset_row and dataset_row.latest_kpi_warnings:
        try:
            warnings_list = json.loads(dataset_row.latest_kpi_warnings)
        except (json.JSONDecodeError, TypeError):
            pass

    data_quality = DataQuality(warnings=warnings_list)

    findings = Findings(
        dataset_id=dataset_id,
        period=month.strftime("%Y-%m"),
        data_quality=data_quality,
        headline=headline,
        movement_summary=movement_summary,
        drivers=drivers,
        cohorts=cohorts,
    )

    payload_bytes = len(findings.model_dump_json().encode("utf-8"))
    if payload_bytes > _FINDINGS_SIZE_WARN_BYTES:
        logger.warning(
            "Findings payload for %s/%s is %d bytes (target < %d). "
            "Consider trimming segment lists.",
            dataset_id,
            month.strftime("%Y-%m"),
            payload_bytes,
            _FINDINGS_SIZE_WARN_BYTES,
        )
    else:
        logger.debug(
            "Findings payload for %s/%s: %d bytes.",
            dataset_id,
            month.strftime("%Y-%m"),
            payload_bytes,
        )

    return findings


def compute_digest_hash(findings: Findings, prompt_version: str) -> str:
    """
    Compute a stable SHA-256 hash over (Findings payload, prompt_version).

    The hash is used as the cache key for LLM responses: identical Findings
    with the same prompt version always produce the same hash, allowing the
    route layer to skip the LLM call and return the cached response.

    Stability guarantees
    --------------------
    - json.dumps with sort_keys=True ensures dict key order is deterministic.
    - prompt_version is appended so that changing the system prompt invalidates
      all existing cache entries without touching the database.

    Parameters
    ----------
    findings       : Fully constructed Findings object.
    prompt_version : Opaque version string (e.g. "insights_v1").

    Returns
    -------
    64-character lowercase hexadecimal SHA-256 digest.
    """
    payload = (
        json.dumps(findings.model_dump(), sort_keys=True, default=str)
        + f"|{prompt_version}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _prev_month(month: date) -> date:
    """Return the first day of the month immediately before `month`."""
    if month.month == 1:
        return date(month.year - 1, 12, 1)
    return date(month.year, month.month - 1, 1)


def _pct_delta(
    curr: Optional[float], prev: Optional[float]
) -> Optional[float]:
    """Percentage change from prev to curr. None if either value is missing/zero."""
    if curr is None or prev is None or prev == 0.0:
        return None
    return round((curr - prev) / prev * 100, 2)


def _pp_delta(
    curr: Optional[float], prev: Optional[float]
) -> Optional[float]:
    """Percentage-point delta (e.g. churn 5% → 8% = +3.0 pp). None if missing."""
    if curr is None or prev is None:
        return None
    return round((curr - prev) * 100, 3)


def _build_segment_rows(curr_rows, prev_rows) -> list[SegmentRow]:
    """
    Join current and previous month segment rows, compute deltas.

    Parameters
    ----------
    curr_rows : KpiSegmentsMonthly ORM objects for the target month.
    prev_rows : KpiSegmentsMonthly ORM objects for the prior month.

    Returns
    -------
    List of SegmentRow with month-over-month delta fields populated where
    a matching prior-month row exists.
    """
    prev_lookup = {
        (r.segment_type, r.segment_value): r for r in prev_rows
    }

    result: list[SegmentRow] = []
    for r in curr_rows:
        prev = prev_lookup.get((r.segment_type, r.segment_value))
        mrr_prev_val = prev.mrr if prev else None
        churn_prev_val = prev.churn_rate if prev else None

        result.append(
            SegmentRow(
                segment_type=r.segment_type,
                segment_value=r.segment_value,
                mrr=r.mrr,
                mrr_prev=mrr_prev_val,
                mrr_delta_pct=_pct_delta(r.mrr, mrr_prev_val),
                churn_rate=r.churn_rate,
                churn_rate_prev=churn_prev_val,
                churn_delta_pp=_pp_delta(r.churn_rate, churn_prev_val),
                mrr_at_risk=r.mrr_at_risk,
            )
        )
    return result


def _build_cohort_points(dataset_id: str, db: Session) -> list[CohortPoint]:
    """
    Fetch M0, M1, and M3 retention snapshots for the two most recent cohorts.

    Age 0 establishes the baseline; ages 1 and 3 surface early and medium-
    term retention signals. Older cohorts are omitted to keep the payload
    compact.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    List of CohortPoint objects, sorted by (cohort_month desc, age_month asc).
    """
    # Identify the two most recent cohort start months
    cohort_month_rows = (
        db.query(models.CohortRetention.cohort_month)
        .filter(models.CohortRetention.dataset_id == dataset_id)
        .distinct()
        .order_by(models.CohortRetention.cohort_month.desc())
        .limit(2)
        .all()
    )
    if not cohort_month_rows:
        return []

    points: list[CohortPoint] = []
    for (cm,) in cohort_month_rows:
        rows = (
            db.query(models.CohortRetention)
            .filter(
                models.CohortRetention.dataset_id == dataset_id,
                models.CohortRetention.cohort_month == cm,
                models.CohortRetention.age_month.in_([0, 1, 3]),
            )
            .order_by(models.CohortRetention.age_month)
            .all()
        )
        for r in rows:
            points.append(
                CohortPoint(
                    cohort_month=r.cohort_month.strftime("%Y-%m"),
                    age_month=r.age_month,
                    retained_pct=r.retained_pct,
                    revenue_retained_pct=r.revenue_retained_pct,
                )
            )
    return points
