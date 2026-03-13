from datetime import date, datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, field_validator


# ---------------------------------------------------------------------------
# Dataset schemas
# ---------------------------------------------------------------------------


class DatasetCreate(BaseModel):
    """Input schema for creating a dataset record (used internally by the service)."""

    name: str


class DatasetResponse(BaseModel):
    """
    Public representation of a Dataset row.

    from_attributes=True (Pydantic v2 equivalent of orm_mode=True) allows
    this schema to be populated directly from a SQLAlchemy ORM object.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    uploaded_at: datetime
    row_count: Optional[int] = None


# ---------------------------------------------------------------------------
# RevenueEvent schemas
# ---------------------------------------------------------------------------


class RevenueEventBase(BaseModel):
    """Shared fields for revenue event schemas."""

    event_date: date
    customer_id: str
    plan: Optional[str] = None
    region: Optional[str] = None
    channel: Optional[str] = None
    event_type: Literal["new", "expansion", "contraction", "churn"]
    amount: float
    signup_date: Optional[date] = None

    @field_validator("amount")
    @classmethod
    def amount_must_be_nonzero(cls, v: float) -> float:
        """
        Revenue events with a zero amount are almost always data quality issues.
        Raise a validation error rather than silently storing bad data.
        """
        if v == 0.0:
            raise ValueError("amount must not be zero")
        return v


class RevenueEventResponse(RevenueEventBase):
    """
    Public representation of a RevenueEvent row, adding the surrogate key
    and foreign key so API consumers can join back to the parent dataset.
    """

    model_config = ConfigDict(from_attributes=True)

    id: str
    dataset_id: str


# ---------------------------------------------------------------------------
# Upload operation response
# ---------------------------------------------------------------------------


class UploadResponse(BaseModel):
    """
    Top-level response returned after a successful CSV upload and processing.

    Fields
    ------
    dataset       : The created Dataset record.
    events_loaded : Number of RevenueEvent rows inserted.
    message       : Human-readable summary string.
    """

    dataset: DatasetResponse
    events_loaded: int
    message: str
