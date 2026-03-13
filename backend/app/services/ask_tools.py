"""
ask_tools.py
------------
Deterministic, LLM-free data retrieval tools for the Ask Agent.

All functions read from pre-computed KPI tables (never raw revenue_events).
Each returns a small dict (<= 2 KB) suitable for injection into an LLM context.

Tools
-----
get_headline        — Current + prior month headline metrics and deltas.
get_top_drivers     — Top 5 churn and MRR-at-risk segments.
get_cohort_points   — Cohort retention summary for 2 cohorts × 3 age points.
run_scenario        — Deterministic MRR/churn projection over a future horizon.
build_compact_context — Combines all tools into a single context dict (<= 8 KB).
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from app.db import models
from app.schemas.scenario import ScenarioSpec

logger = logging.getLogger(__name__)

_CONTEXT_SIZE_WARN_BYTES = 8_192


# ---------------------------------------------------------------------------
# Public tools
# ---------------------------------------------------------------------------


def get_headline(dataset_id: str, month_date: date, db: Session) -> dict:
    """
    Return current and prior-month headline KPI metrics as a compact dict.

    Reads kpi_mrr_monthly and kpi_churn_monthly. Never touches revenue_events.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset.
    month_date : First day of the target month.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    dict with keys: period, mrr, mrr_prev, mrr_delta_pct, new_mrr,
    expansion_mrr, contraction_mrr, churn_mrr, net_new_mrr,
    nrr, nrr_prev, grr, customer_churn_rate, revenue_churn_rate.
    All numeric values are float or None.
    """
    prev = _prev_month(month_date)

    mrr = db.query(models.KpiMrrMonthly).filter_by(dataset_id=dataset_id, month=month_date).first()
    mrr_prev = db.query(models.KpiMrrMonthly).filter_by(dataset_id=dataset_id, month=prev).first()
    churn = db.query(models.KpiChurnMonthly).filter_by(dataset_id=dataset_id, month=month_date).first()
    churn_prev = db.query(models.KpiChurnMonthly).filter_by(dataset_id=dataset_id, month=prev).first()

    mrr_val = mrr.mrr if mrr else None
    mrr_prev_val = mrr_prev.mrr if mrr_prev else None

    return {
        "period": month_date.strftime("%Y-%m"),
        "mrr": _r(mrr_val),
        "mrr_prev": _r(mrr_prev_val),
        "mrr_delta_pct": _pct_delta(mrr_val, mrr_prev_val),
        "new_mrr": _r(mrr.new_mrr if mrr else None),
        "expansion_mrr": _r(mrr.expansion_mrr if mrr else None),
        "contraction_mrr": _r(mrr.contraction_mrr if mrr else None),
        "churn_mrr": _r(mrr.churn_mrr if mrr else None),
        "net_new_mrr": _r(mrr.net_new_mrr if mrr else None),
        "nrr": _r(churn.nrr if churn else None),
        "nrr_prev": _r(churn_prev.nrr if churn_prev else None),
        "nrr_delta_pp": _pp_delta(
            churn.nrr if churn else None,
            churn_prev.nrr if churn_prev else None,
        ),
        "grr": _r(churn.grr if churn else None),
        "customer_churn_rate": _r(churn.customer_churn_rate if churn else None),
        "revenue_churn_rate": _r(churn.revenue_churn_rate if churn else None),
    }


def get_top_drivers(dataset_id: str, month_date: date, db: Session) -> dict:
    """
    Return top 5 churn-risk segments and top 5 MRR-at-risk segments.

    Reads kpi_segments_monthly. Segments are ranked by mrr_at_risk (churn
    risk) and by mrr_delta_pct change (MRR movement). Only the most
    decision-relevant rows are included to keep the payload small.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset.
    month_date : First day of the target month.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    dict with keys: period, top_churn_segments, top_mrr_decline_segments.
    Each is a list of up to 5 dicts.
    """
    prev = _prev_month(month_date)

    curr_rows = (
        db.query(models.KpiSegmentsMonthly)
        .filter_by(dataset_id=dataset_id, month=month_date)
        .all()
    )
    prev_rows = (
        db.query(models.KpiSegmentsMonthly)
        .filter_by(dataset_id=dataset_id, month=prev)
        .all()
    )

    prev_lookup = {(r.segment_type, r.segment_value): r for r in prev_rows}

    enriched = []
    for r in curr_rows:
        p = prev_lookup.get((r.segment_type, r.segment_value))
        mrr_delta_pct = _pct_delta(r.mrr, p.mrr if p else None)
        enriched.append({
            "segment_type": r.segment_type,
            "segment_value": r.segment_value,
            "mrr": _r(r.mrr),
            "mrr_delta_pct": mrr_delta_pct,
            "churn_rate": _r(r.churn_rate),
            "mrr_at_risk": _r(r.mrr_at_risk),
        })

    top_churn = sorted(
        [x for x in enriched if (x["mrr_at_risk"] or 0) > 0],
        key=lambda x: x["mrr_at_risk"] or 0,
        reverse=True,
    )[:5]

    top_decline = sorted(
        [x for x in enriched if (x["mrr_delta_pct"] or 0) < 0],
        key=lambda x: x["mrr_delta_pct"] or 0,
    )[:5]

    return {
        "period": month_date.strftime("%Y-%m"),
        "top_churn_segments": top_churn,
        "top_mrr_decline_segments": top_decline,
    }


def get_cohort_points(dataset_id: str, db: Session) -> dict:
    """
    Return cohort retention summary for the two most recent cohorts.

    Reads cohort_retention at age_month 0, 1, and 3 only to keep the
    payload compact. Age 0 = baseline, age 1 = early retention, age 3 =
    medium-term retention.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    dict with key 'cohorts', a list of up to 6 data points.
    """
    cohort_months = (
        db.query(models.CohortRetention.cohort_month)
        .filter(models.CohortRetention.dataset_id == dataset_id)
        .distinct()
        .order_by(models.CohortRetention.cohort_month.desc())
        .limit(2)
        .all()
    )

    points = []
    for (cm,) in cohort_months:
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
            points.append({
                "cohort_month": r.cohort_month.strftime("%Y-%m"),
                "age_month": r.age_month,
                "retained_pct": _r(r.retained_pct),
                "revenue_retained_pct": _r(r.revenue_retained_pct),
            })

    return {"cohorts": points}


def run_scenario(dataset_id: str, scenario: ScenarioSpec, db: Session) -> dict:
    """
    Run a deterministic MRR projection under a hypothetical KPI change.

    Baseline and scenario MRR are projected month-by-month using:
        MRR_next = MRR_prev × (1 − churn_rate) + avg_new_mrr

    The scenario modifies churn_rate (or avg_new_mrr for metric='new_mrr')
    by the specified amount. 'new_mrr' scenarios apply the change as an
    absolute delta or relative multiplier to avg_new_mrr.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset.
    scenario   : ScenarioSpec describing the hypothetical change.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    dict with baseline and scenario MRR projections, ARR delta, and metadata.
    Returns {'error': '<message>'} if KPI data is unavailable.
    """
    mrr_rows = (
        db.query(models.KpiMrrMonthly)
        .filter_by(dataset_id=dataset_id)
        .order_by(models.KpiMrrMonthly.month.desc())
        .limit(3)
        .all()
    )
    churn_rows = (
        db.query(models.KpiChurnMonthly)
        .filter_by(dataset_id=dataset_id)
        .order_by(models.KpiChurnMonthly.month.desc())
        .limit(2)
        .all()
    )

    if not mrr_rows:
        return {"error": "No KPI data available — run POST /kpis/run first."}

    latest_mrr = mrr_rows[0]
    latest_churn = churn_rows[0] if churn_rows else None

    baseline_mrr_start = latest_mrr.mrr or 0.0
    baseline_churn = (latest_churn.revenue_churn_rate if latest_churn else None) or 0.05
    avg_new_mrr = (
        sum(r.new_mrr or 0.0 for r in mrr_rows) / len(mrr_rows)
    )

    # Compute scenario parameters
    if scenario.metric == "new_mrr":
        scenario_churn = baseline_churn
        if scenario.change_type == "absolute_pp":
            scenario_new_mrr = avg_new_mrr + scenario.value
        else:
            scenario_new_mrr = avg_new_mrr * (1 + scenario.value / 100.0)
    else:
        # Default: churn metric
        if scenario.change_type == "absolute_pp":
            scenario_churn = baseline_churn + scenario.value / 100.0
        else:
            scenario_churn = baseline_churn * (1 + scenario.value / 100.0)
        scenario_new_mrr = avg_new_mrr

    scenario_churn = max(0.0, min(1.0, scenario_churn))
    scenario_new_mrr = max(0.0, scenario_new_mrr)

    # Build projection arrays
    baseline_proj = [round(baseline_mrr_start, 2)]
    scenario_proj = [round(baseline_mrr_start, 2)]

    base_month = latest_mrr.month
    month_labels = [base_month.strftime("%Y-%m")]

    for i in range(scenario.horizon_months):
        b = baseline_proj[-1] * (1.0 - baseline_churn) + avg_new_mrr
        s = scenario_proj[-1] * (1.0 - scenario_churn) + scenario_new_mrr
        baseline_proj.append(round(b, 2))
        scenario_proj.append(round(s, 2))

        # Advance month label
        raw_m = base_month.month + i + 1
        y = base_month.year + (raw_m - 1) // 12
        m = (raw_m - 1) % 12 + 1
        month_labels.append(f"{y}-{m:02d}")

    arr_delta = (scenario_proj[-1] - baseline_proj[-1]) * 12
    arr_delta_pct = (
        round(arr_delta / (baseline_proj[-1] * 12) * 100, 2)
        if baseline_proj[-1] > 0
        else None
    )

    return {
        "metric": scenario.metric,
        "change_type": scenario.change_type,
        "change_value": scenario.value,
        "horizon_months": scenario.horizon_months,
        "baseline_churn_pct": round(baseline_churn * 100, 3),
        "scenario_churn_pct": round(scenario_churn * 100, 3),
        "baseline_new_mrr": round(avg_new_mrr, 2),
        "scenario_new_mrr": round(scenario_new_mrr, 2),
        "months": month_labels,
        "baseline_mrr": baseline_proj,
        "scenario_mrr": scenario_proj,
        "arr_delta": round(arr_delta, 2),
        "arr_delta_pct": arr_delta_pct,
    }


def build_compact_context(
    dataset_id: str,
    month_date: date,
    db: Session,
    include_findings: bool = False,
) -> dict:
    """
    Assemble a combined context dict for LLM consumption.

    Calls get_headline, get_top_drivers, and get_cohort_points and merges
    their outputs into a single dict. Logs a warning if the result exceeds
    the 8 KB target.

    Parameters
    ----------
    dataset_id       : UUID string of the parent Dataset.
    month_date       : First day of the target month.
    db               : Active SQLAlchemy Session.
    include_findings : If True, also call insight_engine.build_findings()
                       and include the full Findings payload. Use only when
                       the extra coverage is worth the token cost.

    Returns
    -------
    Combined context dict suitable for JSON serialisation into an LLM prompt.
    """
    ctx: dict = {
        "headline": get_headline(dataset_id, month_date, db),
        "top_drivers": get_top_drivers(dataset_id, month_date, db),
        "cohorts": get_cohort_points(dataset_id, db),
    }

    if include_findings:
        try:
            from app.services.insight_engine import build_findings
            findings = build_findings(dataset_id, month_date, db)
            ctx["findings"] = findings.model_dump()
        except Exception as exc:
            logger.warning("Could not include findings in context: %s", exc)

    payload_bytes = len(json.dumps(ctx, default=str).encode("utf-8"))
    if payload_bytes > _CONTEXT_SIZE_WARN_BYTES:
        logger.warning(
            "Compact context for %s/%s is %d bytes (target < %d).",
            dataset_id,
            month_date.strftime("%Y-%m"),
            payload_bytes,
            _CONTEXT_SIZE_WARN_BYTES,
        )
    else:
        logger.debug(
            "Compact context for %s/%s: %d bytes.",
            dataset_id,
            month_date.strftime("%Y-%m"),
            payload_bytes,
        )

    return ctx


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _prev_month(month: date) -> date:
    """Return the first day of the month immediately before `month`."""
    if month.month == 1:
        return date(month.year - 1, 12, 1)
    return date(month.year, month.month - 1, 1)


def _r(v: Optional[float]) -> Optional[float]:
    """Round float to 4 decimal places; pass through None."""
    return round(v, 4) if v is not None else None


def _pct_delta(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
    """Percentage change from prev to curr. None if either is missing/zero."""
    if curr is None or prev is None or prev == 0.0:
        return None
    return round((curr - prev) / prev * 100, 2)


def _pp_delta(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
    """Percentage-point delta (e.g. NRR 1.05 → 1.08 = +3.0 pp). None if missing."""
    if curr is None or prev is None:
        return None
    return round((curr - prev) * 100, 3)
