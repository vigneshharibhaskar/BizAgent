import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    DateTime,
    Date,
    ForeignKey,
    Text,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def _new_uuid() -> str:
    """
    Generate a new UUID4 as a string. Used as the default factory for
    primary key columns. Stored as a 36-character hyphenated string
    (e.g. '550e8400-e29b-41d4-a716-446655440000') for SQLite compatibility.
    """
    return str(uuid.uuid4())


class Dataset(Base):
    """
    Represents a single uploaded CSV file. One Dataset has many RevenueEvents.

    Columns
    -------
    id          : UUID primary key, generated at insert time.
    name        : Human-readable label supplied by the caller (e.g. 'Q1 2024').
    uploaded_at : UTC timestamp of the insert; set automatically.
    row_count   : Number of revenue event rows parsed from the CSV.
    """

    __tablename__ = "datasets"

    id = Column(String(36), primary_key=True, default=_new_uuid, nullable=False)
    name = Column(String(255), nullable=False)
    uploaded_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    row_count = Column(Integer, nullable=True)
    # Serialised JSON list of warning strings from the most recent KPI run.
    # Updated by POST /datasets/{id}/kpis/run so insight_engine can include
    # data-quality context in Findings without a separate kpi_run_log table.
    latest_kpi_warnings = Column(Text, nullable=True)

    # cascade="all, delete-orphan": deleting a Dataset also deletes its events.
    events = relationship(
        "RevenueEvent",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )


class RevenueEvent(Base):
    """
    Represents a single row from the uploaded CSV — one atomic revenue event
    for a customer on a given date.

    event_type must be one of: 'new', 'expansion', 'contraction', 'churn'.
    This constraint is enforced at the service layer to produce informative
    validation error messages rather than raw IntegrityErrors.

    Columns
    -------
    id          : UUID primary key, generated at insert time.
    dataset_id  : Foreign key to datasets.id; cascade-deleted with parent.
    event_date  : The date the revenue event occurred.
    customer_id : Opaque customer identifier from the source system.
    plan        : Subscription plan name (e.g. 'starter', 'pro', 'enterprise').
    region      : Optional geographic region tag.
    channel     : Optional acquisition channel tag.
    event_type  : One of {'new', 'expansion', 'contraction', 'churn'}.
    amount      : MRR or ARR delta in the caller's currency.
    signup_date : Date the customer originally signed up.
    """

    __tablename__ = "revenue_events"

    id = Column(String(36), primary_key=True, default=_new_uuid, nullable=False)
    dataset_id = Column(
        String(36),
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_date = Column(Date, nullable=False)
    customer_id = Column(String(255), nullable=False)
    plan = Column(String(100), nullable=True)
    region = Column(String(100), nullable=True)
    channel = Column(String(100), nullable=True)
    event_type = Column(String(20), nullable=False)
    amount = Column(Float, nullable=False)
    signup_date = Column(Date, nullable=True)

    dataset = relationship("Dataset", back_populates="events")


# ---------------------------------------------------------------------------
# KPI result tables — populated by kpi_engine.run_kpis()
# Composite primary keys avoid surrogate key overhead for time-series data.
# All four tables cascade-delete when their parent Dataset is deleted.
# ---------------------------------------------------------------------------


class KpiMrrMonthly(Base):
    """
    Pre-computed MRR component breakdown per dataset per calendar month.

    'month' is always the first day of the month (e.g. 2024-01-01).
    contraction_mrr and churn_mrr are stored as positive absolute values.

    Columns
    -------
    mrr             : Total active MRR at end of month (sum of positive balances).
    new_mrr         : MRR added from new customer events.
    expansion_mrr   : MRR added from expansion events.
    contraction_mrr : Absolute MRR lost to contraction (stored positive).
    churn_mrr       : Absolute MRR lost to churn (stored positive).
    net_new_mrr     : new + expansion - contraction - churn.
    """

    __tablename__ = "kpi_mrr_monthly"

    dataset_id = Column(
        String(36), ForeignKey("datasets.id", ondelete="CASCADE"), primary_key=True
    )
    month = Column(Date, primary_key=True)
    mrr = Column(Float)
    new_mrr = Column(Float)
    expansion_mrr = Column(Float)
    contraction_mrr = Column(Float)
    churn_mrr = Column(Float)
    net_new_mrr = Column(Float)


class KpiChurnMonthly(Base):
    """
    Monthly churn and retention rate metrics per dataset.

    All rate fields are nullable — they are None when the previous month
    has zero active customers or zero starting MRR (division undefined).

    Columns
    -------
    customer_churn_rate : churned_customers / active_customers_prev_month.
    revenue_churn_rate  : churn_mrr / start_mrr.
    grr                 : (start_mrr - contraction - churn) / start_mrr.
    nrr                 : (start_mrr + expansion - contraction - churn) / start_mrr.
    """

    __tablename__ = "kpi_churn_monthly"

    dataset_id = Column(
        String(36), ForeignKey("datasets.id", ondelete="CASCADE"), primary_key=True
    )
    month = Column(Date, primary_key=True)
    customer_churn_rate = Column(Float, nullable=True)
    revenue_churn_rate = Column(Float, nullable=True)
    grr = Column(Float, nullable=True)
    nrr = Column(Float, nullable=True)


class KpiSegmentsMonthly(Base):
    """
    Monthly MRR and churn metrics broken down by a single segment dimension.

    segment_type is one of: 'plan', 'region', 'channel'.
    segment_value is the actual value (e.g. 'pro', 'EMEA', 'organic').
    Rows with null segment_value are excluded during computation.

    Columns
    -------
    mrr         : Total active MRR for this segment at end of month.
    churn_rate  : churned_in_segment / active_prev_in_segment (nullable).
    mrr_at_risk : start_mrr_segment * churn_rate (nullable).
    """

    __tablename__ = "kpi_segments_monthly"

    dataset_id = Column(
        String(36), ForeignKey("datasets.id", ondelete="CASCADE"), primary_key=True
    )
    month = Column(Date, primary_key=True)
    segment_type = Column(String(20), primary_key=True)
    segment_value = Column(String(100), primary_key=True)
    mrr = Column(Float)
    churn_rate = Column(Float, nullable=True)
    mrr_at_risk = Column(Float, nullable=True)


class CohortRetention(Base):
    """
    Cohort retention curve per dataset.

    cohort_month is the month the cohort started (derived from signup_date;
    falls back to first event month if signup_date is null).
    age_month = 0 is the starting month (always 100% retained by definition).

    Columns
    -------
    retained_pct         : % of cohort customers still active at age_month.
    revenue_retained_pct : % of cohort starting MRR still active at age_month.
    """

    __tablename__ = "cohort_retention"

    dataset_id = Column(
        String(36), ForeignKey("datasets.id", ondelete="CASCADE"), primary_key=True
    )
    cohort_month = Column(Date, primary_key=True)
    age_month = Column(Integer, primary_key=True)
    retained_pct = Column(Float, nullable=True)
    revenue_retained_pct = Column(Float, nullable=True)


# ---------------------------------------------------------------------------
# Insights tables — populated by POST /datasets/{id}/insights/generate
# ---------------------------------------------------------------------------


class LlmCache(Base):
    """
    Content-addressed cache for LLM responses.

    Keyed by digest_hash = SHA-256(findings_json + prompt_version) so that
    identical Findings always reuse the same AI response without a second
    API call. Hash collisions are astronomically unlikely with SHA-256.

    Columns
    -------
    digest_hash   : SHA-256 hex digest of (serialised Findings + prompt version).
    response_json : Raw JSON string of the AnalysisResponse returned by the LLM.
    created_at    : UTC timestamp of the first (and only) write.
    """

    __tablename__ = "llm_cache"

    digest_hash = Column(Text, primary_key=True)
    response_json = Column(Text, nullable=False)
    created_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class Insight(Base):
    """
    Reproducible latest snapshot per (dataset_id, month).

    Storage model: overwrite — each generate call replaces the prior row for
    the same (dataset_id, month). Keeps storage minimal; append-mode history
    can be added in v2 if required.

    Stores the Findings input, AnalysisResponse output, and prompt_version
    used so the full chain (KPI aggregates → compact Findings → AI analysis)
    is fully reproducible: given the same digest_hash and prompt_version the
    result is deterministic.

    Columns
    -------
    id             : UUID primary key.
    dataset_id     : Parent dataset; cascade-deleted with it.
    month          : First day of the month the insight covers.
    digest_hash    : Links to llm_cache row (informational, no FK constraint).
    prompt_version : System prompt version used (e.g. 'insights_v1'). Each
                     snapshot is tied to a specific version for reproducibility.
    findings_json  : Serialised Findings payload (aggregates only, no PII).
    response_json  : Serialised AnalysisResponse returned by the LLM.
    created_at     : UTC timestamp of this generation event.
    """

    __tablename__ = "insights"

    id = Column(String(36), primary_key=True, default=_new_uuid, nullable=False)
    dataset_id = Column(
        String(36),
        ForeignKey("datasets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    month = Column(Date, nullable=False)
    digest_hash = Column(Text, nullable=False)
    prompt_version = Column(String(50), nullable=False)
    findings_json = Column(Text, nullable=False)
    response_json = Column(Text, nullable=False)
    created_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
