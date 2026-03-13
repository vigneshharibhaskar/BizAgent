"""
dataset_loader.py
-----------------
Service module for loading revenue CSV files into the database.

Responsibilities:
  - Read CSV files via pandas.
  - Validate schema (required columns) and domain values (event_type).
  - Transform raw strings into typed Python/pandas objects.
  - Insert Dataset and RevenueEvent rows within a single transaction.

This module has NO knowledge of HTTP — all errors are plain Python
exceptions (ValueError for data issues). HTTP mapping is the route layer's job.
"""

import uuid
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.db import models
from app.schemas.dataset import DatasetResponse, UploadResponse

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS: set[str] = {
    "event_date",
    "customer_id",
    "plan",
    "event_type",
    "amount",
    "signup_date",
}

VALID_EVENT_TYPES: set[str] = {"new", "expansion", "contraction", "churn"}

OPTIONAL_COLUMNS: set[str] = {"region", "channel"}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def load_dataset(
    file_path: Path,
    name: str,
    db: Session,
    dataset_id: Optional[str] = None,
) -> UploadResponse:
    """
    Parse, validate, transform, and persist a revenue CSV file.

    Parameters
    ----------
    file_path  : Absolute path to the saved CSV file.
    name       : Human-readable label for this dataset.
    db         : Active SQLAlchemy Session (provided by FastAPI DI).
    dataset_id : Optional pre-generated UUID string. If not provided, a new
                 UUID is generated here. Passing it from the route layer
                 ensures the saved filename and DB primary key match.

    Returns
    -------
    UploadResponse containing the created Dataset and event count.

    Raises
    ------
    ValueError               : For schema violations or invalid domain values.
    pandas.errors.ParserError: If the file is not valid CSV.
    """
    if dataset_id is None:
        dataset_id = str(uuid.uuid4())

    # Step 1 — Read CSV (all columns as str to prevent silent type mangling)
    df = pd.read_csv(file_path, dtype=str)

    # Step 2 — Validate structure and domain values
    validate_schema(df)
    validate_event_types(df)

    # Step 3 — Type conversion and cleaning
    df = transform_rows(df)

    # Step 4 — Write to DB inside a single transaction (all-or-nothing)
    row_count = len(df)
    dataset_orm = insert_dataset(
        name=name,
        row_count=row_count,
        db=db,
        dataset_id=dataset_id,
    )
    events_inserted = insert_revenue_events(df=df, dataset_id=dataset_id, db=db)

    db.commit()  # single commit — atomicity: all-or-nothing

    dataset_response = DatasetResponse.model_validate(dataset_orm)
    return UploadResponse(
        dataset=dataset_response,
        events_loaded=events_inserted,
        message=f"Successfully loaded {events_inserted} revenue events for dataset '{name}'.",
    )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_schema(df: pd.DataFrame) -> None:
    """
    Check that all required columns are present in the DataFrame.

    Column names are normalised to lowercase before checking — handles the
    common real-world issue of inconsistent header capitalisation across
    different CSV exporters. Normalisation is applied in-place so downstream
    functions can rely on lowercase names.

    Parameters
    ----------
    df : Raw DataFrame read from the uploaded CSV.

    Raises
    ------
    ValueError : With a descriptive message listing the missing column names.
    """
    # Normalise in-place first
    df.columns = [col.strip().lower() for col in df.columns]

    actual_columns = set(df.columns)
    missing = REQUIRED_COLUMNS - actual_columns

    if missing:
        raise ValueError(
            f"CSV is missing required columns: {sorted(missing)}. "
            f"Required: {sorted(REQUIRED_COLUMNS)}."
        )


def validate_event_types(df: pd.DataFrame) -> None:
    """
    Check that all values in the 'event_type' column are members of
    VALID_EVENT_TYPES.

    Parameters
    ----------
    df : DataFrame with already-normalised (lowercase) column names.

    Raises
    ------
    ValueError : With the set of offending values to help callers fix their data.
    """
    event_types_in_data = set(
        df["event_type"].str.strip().str.lower().dropna().unique()
    )
    invalid = event_types_in_data - VALID_EVENT_TYPES

    if invalid:
        raise ValueError(
            f"Invalid event_type values found: {sorted(invalid)}. "
            f"Allowed values: {sorted(VALID_EVENT_TYPES)}."
        )


# ---------------------------------------------------------------------------
# Transformation
# ---------------------------------------------------------------------------


def transform_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast all columns to their target Python types and normalise optional columns.

    Transformations applied:
    - event_date  : parsed to datetime.date via pd.to_datetime.
    - signup_date : parsed to datetime.date; NaT/NaN becomes None.
    - amount      : cast to float64; non-numeric rows raise ValueError.
    - event_type  : strip whitespace, lowercase.
    - customer_id : strip whitespace.
    - plan        : strip whitespace; empty string -> None.
    - region      : added as None if column absent; else stripped.
    - channel     : added as None if column absent; else stripped.

    Parameters
    ----------
    df : DataFrame with normalised column names and all values as str.

    Returns
    -------
    A new DataFrame with correctly typed columns.

    Raises
    ------
    ValueError : If date parsing fails or 'amount' contains non-numeric data.
    """
    df = df.copy()

    # Date columns
    try:
        df["event_date"] = pd.to_datetime(
            df["event_date"], infer_datetime_format=True
        ).dt.date
    except Exception as exc:
        raise ValueError(f"Could not parse 'event_date': {exc}") from exc

    try:
        df["signup_date"] = pd.to_datetime(
            df["signup_date"], infer_datetime_format=True, errors="coerce"
        ).dt.date
        # Replace NaN (from NaT after .dt.date) with None for SQLAlchemy
        df["signup_date"] = df["signup_date"].where(
            df["signup_date"].notna(), other=None
        )
    except Exception as exc:
        raise ValueError(f"Could not parse 'signup_date': {exc}") from exc

    # Numeric column
    try:
        df["amount"] = pd.to_numeric(df["amount"], errors="raise")
    except ValueError as exc:
        raise ValueError(
            f"Column 'amount' contains non-numeric data: {exc}"
        ) from exc

    # String normalisation
    df["event_type"] = df["event_type"].str.strip().str.lower()
    df["customer_id"] = df["customer_id"].str.strip()
    df["plan"] = df["plan"].str.strip().replace("", None)

    # Optional columns — add as None if absent
    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = None
        else:
            df[col] = df[col].str.strip().replace("", None)

    return df


# ---------------------------------------------------------------------------
# Database insert helpers
# ---------------------------------------------------------------------------


def insert_dataset(
    name: str,
    row_count: int,
    db: Session,
    dataset_id: str,
) -> models.Dataset:
    """
    Create and flush (but do not commit) a Dataset ORM object.

    Flushing makes the object visible within the current transaction for
    foreign key relationships without an explicit commit.

    Parameters
    ----------
    name       : Human-readable dataset label.
    row_count  : Number of revenue event rows in the CSV.
    db         : Active SQLAlchemy Session.
    dataset_id : Pre-generated UUID string to use as primary key.

    Returns
    -------
    The flushed Dataset ORM instance (id is now populated).
    """
    dataset = models.Dataset(
        id=dataset_id,
        name=name,
        row_count=row_count,
    )
    db.add(dataset)
    db.flush()  # assigns id without committing; allows FK references below
    return dataset


def insert_revenue_events(
    df: pd.DataFrame,
    dataset_id: str,
    db: Session,
) -> int:
    """
    Bulk-insert all revenue event rows from the transformed DataFrame.

    Uses db.bulk_insert_mappings() rather than individual db.add() calls.
    For a 10,000-row CSV this is typically 10-50x faster because it skips
    ORM instance construction overhead and batches the INSERT.

    Parameters
    ----------
    df         : Transformed DataFrame (output of transform_rows).
    dataset_id : UUID string of the parent Dataset record.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    Number of rows inserted (== len(df)).
    """
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "id": str(uuid.uuid4()),
                "dataset_id": dataset_id,
                "event_date": row["event_date"],
                "customer_id": row["customer_id"],
                "plan": row["plan"] if pd.notna(row["plan"]) else None,
                "region": row["region"] if pd.notna(row["region"]) else None,
                "channel": row["channel"] if pd.notna(row["channel"]) else None,
                "event_type": row["event_type"],
                "amount": float(row["amount"]),
                "signup_date": row["signup_date"]
                if pd.notna(row["signup_date"])
                else None,
            }
        )

    db.bulk_insert_mappings(models.RevenueEvent, records)
    return len(records)
