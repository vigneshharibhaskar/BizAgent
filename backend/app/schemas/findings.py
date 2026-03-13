"""
findings.py
-----------
Pydantic schemas for the compact Findings payload built deterministically
from KPI tables and passed to the AI insight engine.

Design constraints
------------------
- All fields are derived from pre-computed KPI aggregates only.
  Raw revenue_events rows are never included.
- Optional fields are common; callers must handle None gracefully.
- Payload is capped by selecting top-N rows per list (see insight_engine.py).
  Aim: < 8 KB serialised JSON so the full payload fits in one LLM context.
"""

from typing import Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Sub-schemas
# ---------------------------------------------------------------------------


class DataQuality(BaseModel):
    """
    Data quality context surfaced to the LLM via the assumptions field.

    warnings : Strings from _validate_kpi_results() checks [A]-[F].
               Empty list means all checks passed cleanly.
    """

    warnings: list[str] = []


class HeadlineMetrics(BaseModel):
    """
    Key MRR and churn metrics for the target month plus prior-month deltas.

    month                    : Target period in YYYY-MM format.
    mrr / mrr_prev           : End-of-month MRR (reporting_mrr floor).
    mrr_delta_pct            : Percentage change vs. previous month.
    new/expansion/contraction/churn/net_new_mrr : MRR movement components.
    customer_churn_rate      : Churned customers / prev-month active customers.
    customer_churn_delta_pp  : Change in churn rate in percentage points.
    revenue_churn_rate       : churn_mrr / start_mrr.
    grr / nrr                : Gross / Net Revenue Retention (dimensionless).
    *_prev                   : Prior-month values for the same rate metrics.
    """

    month: str  # YYYY-MM
    mrr: Optional[float] = None
    mrr_prev: Optional[float] = None
    mrr_delta_pct: Optional[float] = None
    new_mrr: Optional[float] = None
    expansion_mrr: Optional[float] = None
    contraction_mrr: Optional[float] = None
    churn_mrr: Optional[float] = None
    net_new_mrr: Optional[float] = None
    customer_churn_rate: Optional[float] = None
    customer_churn_rate_prev: Optional[float] = None
    customer_churn_delta_pp: Optional[float] = None
    revenue_churn_rate: Optional[float] = None
    grr: Optional[float] = None
    grr_prev: Optional[float] = None
    nrr: Optional[float] = None
    nrr_prev: Optional[float] = None


class SegmentRow(BaseModel):
    """
    MRR and churn metrics for one (segment_type, segment_value) pair,
    including month-over-month deltas where a prior-month row exists.

    segment_type  : Dimension name — 'plan', 'region', or 'channel'.
    segment_value : Dimension value — e.g. 'enterprise', 'EMEA', 'paid'.
    mrr_delta_pct : Percentage change in MRR vs. previous month (None if no prior row).
    churn_delta_pp: Change in churn rate in percentage points (None if no prior row).
    mrr_at_risk   : start_mrr * churn_rate — MRR likely to be lost next month.
    """

    segment_type: str
    segment_value: str
    mrr: Optional[float] = None
    mrr_prev: Optional[float] = None
    mrr_delta_pct: Optional[float] = None
    churn_rate: Optional[float] = None
    churn_rate_prev: Optional[float] = None
    churn_delta_pp: Optional[float] = None
    mrr_at_risk: Optional[float] = None


class MovementSummary(BaseModel):
    """
    Top movers across all segment dimensions for the target month.

    top_positive : Up to 3 segments with the highest positive mrr_delta_pct.
    top_negative : Up to 3 segments with the largest negative mrr_delta_pct.
    Only segments with a valid prior-month row are included.
    """

    top_positive: list[SegmentRow] = []
    top_negative: list[SegmentRow] = []


class Drivers(BaseModel):
    """
    Segments most likely driving churn and MRR decline.

    churn_segments       : Top 5 by mrr_at_risk descending.
    mrr_decline_segments : Top 5 by most negative mrr_delta_pct.
    """

    churn_segments: list[SegmentRow] = []
    mrr_decline_segments: list[SegmentRow] = []


class CohortPoint(BaseModel):
    """
    Single retention data point for one cohort at one age.

    cohort_month         : Cohort start month in YYYY-MM format.
    age_month            : Months since cohort start (0 = starting month).
    retained_pct         : % of cohort customers still active.
    revenue_retained_pct : % of cohort starting MRR still active.
    """

    cohort_month: str  # YYYY-MM
    age_month: int
    retained_pct: Optional[float] = None
    revenue_retained_pct: Optional[float] = None


class Cohorts(BaseModel):
    """
    Retention snapshots for the two most recent cohorts at ages 0, 1, and 3.
    Provides early-warning signal for long-term retention health.
    """

    points: list[CohortPoint] = []


class Anomaly(BaseModel):
    """Optional anomaly detected during Findings construction."""

    description: str
    metric: Optional[str] = None
    value: Optional[float] = None


# ---------------------------------------------------------------------------
# Top-level Findings
# ---------------------------------------------------------------------------


class Findings(BaseModel):
    """
    Complete compact KPI summary for one (dataset, month) pair.

    This is the sole input to the AI insight engine. It is built
    deterministically from KPI tables — never from raw revenue_events.

    Fields
    ------
    schema_version   : Semver string; increment when structure changes.
    dataset_id       : UUID of the parent Dataset.
    period           : Target month in YYYY-MM format.
    data_quality     : KPI validation warnings for the dataset.
    headline         : Core MRR and churn metrics with prior-month deltas.
    movement_summary : Fastest-growing and fastest-shrinking segments.
    drivers          : Top churn-risk and MRR-decline segments.
    retention        : Reserved for future tenure-based retention metrics.
    cohorts          : Retention snapshots for the two latest cohorts.
    anomalies        : Optional list of detected anomalies.
    notes            : Free-text context strings appended by the engine.
    """

    schema_version: str = "1.0"
    dataset_id: str
    period: str  # YYYY-MM
    data_quality: DataQuality
    headline: HeadlineMetrics
    movement_summary: MovementSummary
    drivers: Drivers
    retention: dict = {}
    cohorts: Cohorts
    anomalies: list[Anomaly] = []
    notes: list[str] = []
