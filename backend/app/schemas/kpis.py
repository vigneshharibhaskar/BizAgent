from datetime import date
from typing import Optional

from pydantic import BaseModel, ConfigDict


# ---------------------------------------------------------------------------
# KPI run response
# ---------------------------------------------------------------------------


class KpiRunResponse(BaseModel):
    """
    Summary returned after a successful KPI computation run.

    Fields
    ------
    dataset_id        : The dataset the KPIs were computed for.
    months_computed   : Number of monthly MRR rows stored.
    segments_computed : Number of (month, segment_type, segment_value) rows stored.
    cohorts_computed  : Number of cohort retention data points stored.
    message           : Human-readable summary.
    warnings          : List of data quality or consistency warnings.
                        Empty list means all checks passed.
                        Prefixed with [A]/[B]/[C]/[D] for the check type.
    """

    dataset_id: str
    months_computed: int
    segments_computed: int
    cohorts_computed: int
    message: str
    warnings: list[str] = []


# ---------------------------------------------------------------------------
# KPI result read schemas
# ---------------------------------------------------------------------------


class MrrMonthlyResponse(BaseModel):
    """Monthly MRR component breakdown."""

    model_config = ConfigDict(from_attributes=True)

    month: date
    mrr: Optional[float] = None
    new_mrr: Optional[float] = None
    expansion_mrr: Optional[float] = None
    contraction_mrr: Optional[float] = None
    churn_mrr: Optional[float] = None
    net_new_mrr: Optional[float] = None


class ChurnMonthlyResponse(BaseModel):
    """Monthly customer and revenue churn, GRR, and NRR."""

    model_config = ConfigDict(from_attributes=True)

    month: date
    customer_churn_rate: Optional[float] = None
    revenue_churn_rate: Optional[float] = None
    grr: Optional[float] = None
    nrr: Optional[float] = None


class SegmentMonthlyResponse(BaseModel):
    """Monthly MRR and churn broken down by a single segment dimension."""

    model_config = ConfigDict(from_attributes=True)

    month: date
    segment_type: str
    segment_value: str
    mrr: Optional[float] = None
    churn_rate: Optional[float] = None
    mrr_at_risk: Optional[float] = None


class CohortRetentionResponse(BaseModel):
    """Single data point on a cohort retention curve."""

    model_config = ConfigDict(from_attributes=True)

    cohort_month: date
    age_month: int
    retained_pct: Optional[float] = None
    revenue_retained_pct: Optional[float] = None
