"""
kpi_engine.py
-------------
Service module that computes all SaaS KPIs from raw revenue_events data.

Responsibilities:
  - Load revenue events from the database into pandas DataFrames.
  - Build a per-customer MRR balance timeline (the shared core data structure).
  - Compute four KPI categories: MRR components, churn metrics, segment
    breakdowns, and cohort retention curves.
  - Upsert (delete + bulk-insert) results into the four KPI tables.

This module has NO knowledge of HTTP. All errors are plain Python exceptions.
The caller (route layer) is responsible for HTTP status code mapping.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from app.db import models

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_kpis(dataset_id: str, db: Session) -> dict:
    """
    Orchestrate the full KPI computation pipeline for one dataset.

    Loads raw events, builds the MRR timeline, computes all four KPI
    categories, replaces any previously stored results, and commits
    everything in a single atomic transaction.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset record.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    dict with keys: months_computed, segments_computed, cohorts_computed.

    Raises
    ------
    ValueError : If no events exist for the given dataset_id.
    """
    df = _load_events(dataset_id, db)

    mrr_timeline = _build_customer_mrr_timeline(df)

    mrr_df = compute_monthly_mrr_components(df, mrr_timeline)
    churn_df = compute_monthly_churn_metrics(df, mrr_timeline)
    seg_df = compute_segment_metrics(df, mrr_timeline)
    cohort_df = compute_cohort_retention_points(df, mrr_timeline)

    warnings = _validate_kpi_results(mrr_df, churn_df, seg_df, mrr_timeline=mrr_timeline, raw_df=df)

    # Replace all previously stored KPI results for this dataset atomically.
    _upsert_mrr(dataset_id, mrr_df, db)
    _upsert_churn(dataset_id, churn_df, db)
    _upsert_segments(dataset_id, seg_df, db)
    _upsert_cohorts(dataset_id, cohort_df, db)

    db.commit()

    return {
        "months_computed": len(mrr_df),
        "segments_computed": len(seg_df),
        "cohorts_computed": len(cohort_df),
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def _load_events(dataset_id: str, db: Session) -> pd.DataFrame:
    """
    Query all revenue events for a dataset and return them as a DataFrame.

    Parameters
    ----------
    dataset_id : UUID string of the target Dataset.
    db         : Active SQLAlchemy Session.

    Returns
    -------
    DataFrame with columns matching RevenueEvent fields.

    Raises
    ------
    ValueError : If no events are found for dataset_id.
    """
    rows = (
        db.query(models.RevenueEvent)
        .filter(models.RevenueEvent.dataset_id == dataset_id)
        .all()
    )

    if not rows:
        raise ValueError(f"No revenue events found for dataset '{dataset_id}'.")

    df = pd.DataFrame(
        [
            {
                "event_date": r.event_date,
                "customer_id": r.customer_id,
                "amount": float(r.amount),
                "event_type": r.event_type,
                "plan": r.plan,
                "region": r.region,
                "channel": r.channel,
                "signup_date": r.signup_date,
            }
            for r in rows
        ]
    )

    df["event_date"] = pd.to_datetime(df["event_date"])
    return df


# ---------------------------------------------------------------------------
# Core shared data structure
# ---------------------------------------------------------------------------


def _build_customer_mrr_timeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a complete customer × month MRR balance grid.

    This is the central data structure consumed by all four KPI computation
    functions. It reconstructs each customer's running MRR from the raw
    delta events and fills in zero-delta months so every customer has a
    row for every month in the dataset's date range.

    Algorithm
    ---------
    1. Assign a Period month to each event.
    2. Sum deltas per (customer, month).
    3. Reindex to the full (customer × month) Cartesian product, filling
       months with no events as 0.
    4. Cumsum per customer across time → mrr_balance (running total).
    5. Attach last-known plan/region/channel per customer per month,
       forward-filled so every row has a segment value.

    Parameters
    ----------
    df : Raw events DataFrame from _load_events().

    Returns
    -------
    DataFrame with columns:
        customer_id, month (Period[M]), delta, mrr_balance,
        reporting_mrr, plan, region, channel

    Columns
    -------
    mrr_balance   : Raw cumulative delta. Can be negative if a customer is
                    over-contracted (e.g. refunded more than they ever paid).
                    Kept for auditability and check [E] in the validator.
    reporting_mrr : mrr_balance clamped to 0 (max(raw, 0)). This is the
                    finance-standard MRR floor used by all KPI computations.
                    Prevents negative balances from distorting MRR totals and
                    churn rates, and eliminates the [B] identity drift that
                    raw balances can cause.
    """
    work = df.copy()
    work["month"] = work["event_date"].dt.to_period("M")

    # --- Step 1: monthly delta per customer ---
    delta = (
        work.groupby(["customer_id", "month"])["amount"]
        .sum()
        .rename("delta")
    )

    # --- Step 2: full customer × month grid ---
    all_months = pd.period_range(work["month"].min(), work["month"].max(), freq="M")
    all_customers = work["customer_id"].unique()

    full_idx = pd.MultiIndex.from_product(
        [all_customers, all_months], names=["customer_id", "month"]
    )
    full = delta.reindex(full_idx, fill_value=0.0).reset_index()

    # --- Step 3: cumulative MRR balance per customer ---
    full = full.sort_values(["customer_id", "month"])
    full["mrr_balance"] = full.groupby("customer_id")["delta"].cumsum()

    # --- Step 4: reporting MRR = balance floored at 0 ---
    # Raw balances are preserved in mrr_balance for auditability.
    # All KPI computations use reporting_mrr so negative-balance months
    # are treated as inactive (not distorting totals or rate denominators).
    full["reporting_mrr"] = full["mrr_balance"].clip(lower=0.0)

    # --- Step 5: attach forward-filled segment attributes ---
    # Take the last event per (customer, month) to get the most recent segment.
    seg = (
        work.sort_values("event_date")
        .groupby(["customer_id", "month"])[["plan", "region", "channel"]]
        .last()
        .reset_index()
    )
    full = full.merge(seg, on=["customer_id", "month"], how="left")

    for col in ["plan", "region", "channel"]:
        full[col] = full.groupby("customer_id")[col].ffill()

    return full.reset_index(drop=True)


# ---------------------------------------------------------------------------
# KPI computations
# ---------------------------------------------------------------------------


def compute_monthly_mrr_components(
    df: pd.DataFrame, mrr_timeline: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute MRR and its growth components for each calendar month.

    MRR = sum of all positive customer balances at end of month.
    Component amounts come directly from event-type aggregates.
    contraction_mrr and churn_mrr are stored as positive values
    (absolute of the negative deltas in the source data).

    Parameters
    ----------
    df           : Raw events DataFrame.
    mrr_timeline : Output of _build_customer_mrr_timeline().

    Returns
    -------
    DataFrame with columns:
        month (date), mrr, new_mrr, expansion_mrr, contraction_mrr,
        churn_mrr, net_new_mrr
    """
    work = df.copy()
    work["month"] = work["event_date"].dt.to_period("M")

    # Total MRR per month = sum of reporting_mrr (already 0-floored).
    # Equivalent to summing positive balances but avoids the filter step.
    mrr_series = (
        mrr_timeline.groupby("month")["reporting_mrr"]
        .sum()
        .rename("mrr")
    )

    # Event-type aggregates per month
    pivot = (
        work.groupby(["month", "event_type"])["amount"]
        .sum()
        .unstack(fill_value=0.0)
    )

    # Ensure all four event-type columns are present
    for et in ["new", "expansion", "contraction", "churn"]:
        if et not in pivot.columns:
            pivot[et] = 0.0

    result = mrr_series.to_frame().join(pivot, how="outer").fillna(0.0)
    result = result.rename(
        columns={
            "new": "new_mrr",
            "expansion": "expansion_mrr",
            "contraction": "contraction_mrr",
            "churn": "churn_mrr",
        }
    )

    # Contraction and churn are negative deltas; store as positive values
    result["contraction_mrr"] = result["contraction_mrr"].abs()
    result["churn_mrr"] = result["churn_mrr"].abs()

    result["net_new_mrr"] = (
        result["new_mrr"]
        + result["expansion_mrr"]
        - result["contraction_mrr"]
        - result["churn_mrr"]
    )

    result = result.reset_index()
    result["month"] = result["month"].dt.to_timestamp().dt.date

    return result[
        ["month", "mrr", "new_mrr", "expansion_mrr", "contraction_mrr",
         "churn_mrr", "net_new_mrr"]
    ]


def compute_monthly_churn_metrics(
    df: pd.DataFrame, mrr_timeline: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute customer and revenue churn, GRR, and NRR for each month.

    All four rate metrics are computed relative to the *previous* month,
    so the first month in the dataset always returns no rows (no prior
    state to compare against).

    Fully vectorised using a customer × month pivot matrix — no Python loops.

    Parameters
    ----------
    df           : Raw events DataFrame.
    mrr_timeline : Output of _build_customer_mrr_timeline().

    Returns
    -------
    DataFrame with columns:
        month (date), customer_churn_rate, revenue_churn_rate, grr, nrr
    All rate columns are nullable (None when denominator is zero).
    """
    work = df.copy()
    work["month"] = work["event_date"].dt.to_period("M")

    # --- Pivot: rows=months, cols=customers, values=reporting_mrr ---
    # reporting_mrr is already 0-floored so no additional clip is needed.
    mrr_pivot = mrr_timeline.pivot_table(
        index="month", columns="customer_id", values="reporting_mrr", fill_value=0.0
    )
    mrr_prev = mrr_pivot.shift(1)  # previous month state; first row is all NaN

    active_curr = mrr_pivot > 0
    active_prev = mrr_prev > 0

    # Churned: was active last month, not active this month
    churned = active_prev & ~active_curr
    n_active_prev = active_prev.sum(axis=1).replace(0, np.nan)
    customer_churn_rate = churned.sum(axis=1) / n_active_prev

    # Start MRR: sum of previous-month reporting balances (already >= 0)
    start_mrr = mrr_prev.sum(axis=1).replace(0, np.nan)

    # Event-type MRR aggregates per month (indexed by Period)
    ev = work.groupby(["month", "event_type"])["amount"].sum().unstack(fill_value=0.0)
    for et in ["expansion", "contraction", "churn"]:
        if et not in ev.columns:
            ev[et] = 0.0

    ev["contraction"] = ev["contraction"].abs()
    ev["churn"] = ev["churn"].abs()

    # Align to mrr_pivot index (full month range)
    ev = ev.reindex(mrr_pivot.index, fill_value=0.0)

    revenue_churn_rate = ev["churn"] / start_mrr
    grr = (start_mrr - ev["contraction"] - ev["churn"]) / start_mrr
    nrr = (start_mrr + ev["expansion"] - ev["contraction"] - ev["churn"]) / start_mrr

    result = pd.DataFrame(
        {
            "month": mrr_pivot.index,
            "customer_churn_rate": customer_churn_rate.values,
            "revenue_churn_rate": revenue_churn_rate.values,
            "grr": grr.values,
            "nrr": nrr.values,
        }
    )

    # Drop first month (all NaN — no previous-month baseline)
    result = result.dropna(subset=["customer_churn_rate"], how="all")

    # Replace NaN with None for SQLAlchemy nullable columns
    result = result.where(pd.notna(result), other=None)
    result["month"] = result["month"].dt.to_timestamp().dt.date

    return result.reset_index(drop=True)


def compute_segment_metrics(
    df: pd.DataFrame, mrr_timeline: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute MRR and churn rate for each (month, segment_type, segment_value).

    Iterates over three segment dimensions: 'plan', 'region', 'channel'.
    Customers with a null segment value for a given dimension are excluded.

    Attribution rules (standard SaaS convention):
    - MRR is attributed to the customer's *current-month* segment value.
    - Churn and active-customer counts are attributed to the customer's
      *previous-month* segment value (the segment they were in when active,
      not the segment they would have migrated to).

    This means a customer who upgraded from 'starter' to 'enterprise' in
    April is counted under 'starter' for April churn calculations (they
    were a 'starter' customer until end of March) and under 'enterprise'
    for April MRR (their balance now belongs to enterprise).

    Parameters
    ----------
    df           : Raw events DataFrame (unused but kept for API symmetry).
    mrr_timeline : Output of _build_customer_mrr_timeline().

    Returns
    -------
    DataFrame with columns:
        month (date), segment_type (str), segment_value (str),
        mrr (float), churn_rate (float|None), mrr_at_risk (float|None)
    """
    tl = mrr_timeline.copy().sort_values(["customer_id", "month"])

    # Compute per-row previous-month state by shifting within each customer group.
    # This gives us the correct prev values even if months are non-contiguous
    # (they won't be, given the full grid built in _build_customer_mrr_timeline,
    # but defensive shifting per-customer is safer than a global pivot shift).
    tl["prev_reporting_mrr"] = tl.groupby("customer_id")["reporting_mrr"].shift(1)
    for seg_col in ["plan", "region", "channel"]:
        tl[f"prev_{seg_col}"] = tl.groupby("customer_id")[seg_col].shift(1)

    # Active state flags (reporting_mrr is already 0-floored)
    tl["active_curr"] = tl["reporting_mrr"] > 0
    tl["active_prev"] = tl["prev_reporting_mrr"].fillna(0.0) > 0
    tl["churned"] = tl["active_prev"] & ~tl["active_curr"]

    all_results: list[pd.DataFrame] = []

    for seg_col in ["plan", "region", "channel"]:
        prev_seg_col = f"prev_{seg_col}"

        # --- MRR: attribute to current-month segment ---
        active_curr_rows = tl[tl["active_curr"] & tl[seg_col].notna()]
        seg_mrr = (
            active_curr_rows.groupby(["month", seg_col])["reporting_mrr"]
            .sum()
            .reset_index()
            .rename(columns={seg_col: "segment_value", "reporting_mrr": "mrr"})
        )
        seg_mrr["segment_type"] = seg_col

        # --- Active prev: attribute to previous-month segment ---
        active_prev_rows = tl[tl["active_prev"] & tl[prev_seg_col].notna()]
        n_active_prev = (
            active_prev_rows.groupby(["month", prev_seg_col])["customer_id"]
            .nunique()
            .reset_index()
            .rename(columns={prev_seg_col: "segment_value", "customer_id": "n_active_prev"})
        )
        # Start MRR = sum of previous-month reporting balances per segment
        start_mrr_seg = (
            active_prev_rows.groupby(["month", prev_seg_col])["prev_reporting_mrr"]
            .sum()
            .reset_index()
            .rename(columns={prev_seg_col: "segment_value", "prev_reporting_mrr": "start_mrr"})
        )

        # --- Churned: attribute to previous-month segment ---
        churned_rows = tl[tl["churned"] & tl[prev_seg_col].notna()]
        n_churned = (
            churned_rows.groupby(["month", prev_seg_col])["customer_id"]
            .nunique()
            .reset_index()
            .rename(columns={prev_seg_col: "segment_value", "customer_id": "n_churned"})
        )

        # --- Merge and compute rates ---
        seg = seg_mrr.merge(
            n_active_prev, on=["month", "segment_value"], how="outer"
        ).merge(
            n_churned, on=["month", "segment_value"], how="outer"
        ).merge(
            start_mrr_seg, on=["month", "segment_value"], how="outer"
        )

        seg["segment_type"] = seg_col
        seg["n_active_prev"] = seg["n_active_prev"].fillna(0.0)
        seg["n_churned"] = seg["n_churned"].fillna(0.0)

        seg["churn_rate"] = np.where(
            seg["n_active_prev"] > 0,
            seg["n_churned"] / seg["n_active_prev"],
            np.nan,
        )
        seg["mrr_at_risk"] = np.where(
            seg["start_mrr"].notna() & (seg["start_mrr"] > 0),
            seg["start_mrr"] * seg["churn_rate"],
            np.nan,
        )

        # Convert Period → first-of-month date
        seg["month"] = seg["month"].dt.to_timestamp().dt.date

        # Replace NaN with None for SQLAlchemy
        seg = seg.where(pd.notna(seg), other=None)

        all_results.append(
            seg[["month", "segment_type", "segment_value",
                 "mrr", "churn_rate", "mrr_at_risk"]]
        )

    if not all_results:
        return pd.DataFrame(
            columns=["month", "segment_type", "segment_value",
                     "mrr", "churn_rate", "mrr_at_risk"]
        )

    return pd.concat(all_results, ignore_index=True)


def compute_cohort_retention_points(
    df: pd.DataFrame, mrr_timeline: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute customer and revenue retention curves by signup cohort.

    Each cohort is defined by the month of the customer's signup_date.
    If signup_date is null, the customer's first event month is used as
    a fallback so no customers are excluded from cohort analysis.

    age_month = 0 corresponds to the cohort's starting month. A customer
    is considered 'retained' at age N if their mrr_balance > 0 at that month.

    Parameters
    ----------
    df           : Raw events DataFrame.
    mrr_timeline : Output of _build_customer_mrr_timeline().

    Returns
    -------
    DataFrame with columns:
        cohort_month (date), age_month (int),
        retained_pct (float|None), revenue_retained_pct (float|None)
    """
    work = df.copy()
    work["month"] = work["event_date"].dt.to_period("M")

    # --- Determine cohort month per customer ---
    work["cohort_month_raw"] = pd.to_datetime(
        work["signup_date"], errors="coerce"
    ).dt.to_period("M")

    # First event month as fallback for missing signup_date
    first_event_month = work.groupby("customer_id")["month"].min()

    customer_cohort_raw = (
        work.sort_values("event_date")
        .groupby("customer_id")["cohort_month_raw"]
        .first()
    )
    # Use signup-based cohort where available; fall back to first event month
    customer_cohort = customer_cohort_raw.where(
        customer_cohort_raw.notna(), first_event_month
    )

    # --- Merge cohort month into timeline ---
    tl = mrr_timeline.copy()
    tl["cohort_month"] = tl["customer_id"].map(customer_cohort)

    # Drop rows where cohort assignment failed (should not happen after fallback)
    tl = tl.dropna(subset=["cohort_month"])

    # age_month = number of months since cohort start
    tl["age_month"] = tl.apply(
        lambda r: int((r["month"] - r["cohort_month"]).n), axis=1
    )

    # Only retain non-negative ages (discard events before cohort month)
    tl = tl[tl["age_month"] >= 0]

    # --- Cohort baseline (age_month == 0) ---
    cohort_base = tl[tl["age_month"] == 0]
    cohort_size = cohort_base.groupby("cohort_month")["customer_id"].nunique()
    cohort_mrr0 = cohort_base.groupby("cohort_month")["reporting_mrr"].sum().replace(
        0, np.nan
    )

    # --- Active customers / MRR at each (cohort_month, age_month) ---
    active_tl = tl[tl["reporting_mrr"] > 0]

    ret_customers = (
        active_tl.groupby(["cohort_month", "age_month"])["customer_id"]
        .nunique()
        .reset_index(name="n_retained")
    )
    ret_mrr = (
        active_tl.groupby(["cohort_month", "age_month"])["reporting_mrr"]
        .sum()
        .reset_index(name="mrr_retained")
    )

    result = ret_customers.merge(ret_mrr, on=["cohort_month", "age_month"], how="outer")

    result["retained_pct"] = result.apply(
        lambda r: r["n_retained"] / cohort_size.get(r["cohort_month"], np.nan)
        if pd.notna(cohort_size.get(r["cohort_month"], np.nan))
           and cohort_size.get(r["cohort_month"]) > 0
        else None,
        axis=1,
    )
    result["revenue_retained_pct"] = result.apply(
        lambda r: r["mrr_retained"] / cohort_mrr0.get(r["cohort_month"], np.nan)
        if pd.notna(cohort_mrr0.get(r["cohort_month"], np.nan))
        else None,
        axis=1,
    )

    result["cohort_month"] = result["cohort_month"].dt.to_timestamp().dt.date

    return result[
        ["cohort_month", "age_month", "retained_pct", "revenue_retained_pct"]
    ].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Post-computation validation
# ---------------------------------------------------------------------------


def _validate_kpi_results(
    mrr_df: pd.DataFrame,
    churn_df: pd.DataFrame,
    seg_df: pd.DataFrame,
    mrr_timeline: Optional[pd.DataFrame] = None,
    raw_df: Optional[pd.DataFrame] = None,
    abs_tol: float = 1e-6,
    rel_tol: float = 0.02,
    seg_tol: float = 0.05,
) -> list[str]:
    """
    Run six algebraic consistency checks against the computed KPI DataFrames.

    All checks produce warnings (strings) rather than exceptions — a warning
    does not prevent results from being stored. Warnings indicate either a
    data quality issue in the source CSV or a genuine edge case the caller
    should investigate.

    Checks
    ------
    A  Net New MRR identity
       net_new_mrr must equal new + expansion - contraction - churn within
       floating-point epsilon. Any violation is a computation bug.

    B  Ending MRR identity (approximate)
       mrr[t] ≈ mrr[t-1] + net_new_mrr[t] within `rel_tol` (default 2%).
       Because KPIs now use reporting_mrr (0-floored), this check should
       almost never fire. A violation means a logic regression.

    C  Rate bounds
       customer_churn_rate ∈ [0, 1]
       revenue_churn_rate ∈ [0, 1]  (can exceed 1 only with over-credited churns)
       grr ∈ [0, 1]
       nrr ≥ 0  (values > 1 are healthy and expected)

    D  Segment MRR sum ≈ total MRR
       For each segment dimension the sum of segment MRR values per month
       should be within `seg_tol` (default 5%) of total MRR. Larger gaps
       are expected when many customers have null values for that dimension.

    E  Negative raw MRR balances
       Detects customers whose raw mrr_balance dipped below zero (over-
       contracted / over-refunded). Reported as informational — reporting_mrr
       clamps these to 0 so they do not affect KPI outputs, but the source
       data may need investigation.

    F  Event sign convention
       new/expansion events should have positive amounts; contraction/churn
       events should have negative amounts. Warns with row counts per
       event_type when the convention is violated.

    Parameters
    ----------
    mrr_df       : Output of compute_monthly_mrr_components().
    churn_df     : Output of compute_monthly_churn_metrics().
    seg_df       : Output of compute_segment_metrics().
    mrr_timeline : Output of _build_customer_mrr_timeline() — required for [E].
    raw_df       : Output of _load_events() — required for [F].
    abs_tol      : Absolute tolerance for check A (floating-point identity).
    rel_tol      : Relative tolerance for check B (MRR ending balance).
    seg_tol      : Max fractional discrepancy allowed in check D before warning.

    Returns
    -------
    List of warning strings (empty list = all checks passed).
    """
    warnings: list[str] = []

    if mrr_df.empty:
        return warnings

    mrr = mrr_df.copy().sort_values("month").reset_index(drop=True)

    # ------------------------------------------------------------------
    # A: Net New MRR identity
    # net_new_mrr is computed from its components, so this should hold to
    # floating-point epsilon.  Any failure is a bug, not a data issue.
    # ------------------------------------------------------------------
    computed_net = (
        mrr["new_mrr"]
        + mrr["expansion_mrr"]
        - mrr["contraction_mrr"]
        - mrr["churn_mrr"]
    )
    identity_diff = (computed_net - mrr["net_new_mrr"]).abs()
    bad_a = mrr.loc[identity_diff > abs_tol, "month"].tolist()
    if bad_a:
        warnings.append(
            f"[A] net_new_mrr identity violated for months: {bad_a}. "
            f"Max deviation: {identity_diff.max():.2e}. This is a computation bug."
        )

    # ------------------------------------------------------------------
    # B: Ending MRR identity (approximate)
    # mrr[t] ≈ mrr[t-1] + net_new_mrr[t]
    # Checked as a relative error against max(mrr[t], 1) to avoid noise
    # on near-zero MRR months.
    # ------------------------------------------------------------------
    prev_mrr = mrr["mrr"].shift(1)
    expected_mrr = prev_mrr + mrr["net_new_mrr"]
    denom = mrr["mrr"].clip(lower=1.0)
    rel_err = (mrr["mrr"] - expected_mrr).abs() / denom

    # Only validate months where a previous-month baseline exists
    mask_b = prev_mrr.notna() & (rel_err > rel_tol)
    bad_b = mrr.loc[mask_b, "month"].tolist()
    if bad_b:
        max_err_pct = rel_err[prev_mrr.notna()].max() * 100
        warnings.append(
            f"[B] Ending MRR identity off by >{rel_tol:.0%} for months: {bad_b}. "
            f"Max relative error: {max_err_pct:.1f}%. "
            f"Unexpected — investigate timeline aggregation / clipping propagation."
        )

    # ------------------------------------------------------------------
    # C: Rate bounds
    # ------------------------------------------------------------------
    if not churn_df.empty:
        rate_checks: list[tuple[str, float, float | None, str]] = [
            ("customer_churn_rate", 0.0, 1.0, "should be in [0, 1]"),
            (
                "revenue_churn_rate",
                0.0,
                1.0,
                "can exceed 1 only with over-credited churn events — consider clamping",
            ),
            ("grr", 0.0, 1.0, "should be in [0, 1]"),
            ("nrr", 0.0, None, "should be >= 0; values > 1 are healthy (net expansion)"),
        ]

        for col, lo, hi, note in rate_checks:
            series = churn_df[col].dropna()
            if series.empty:
                continue

            out_lo = series[series < lo]
            out_hi = series[series > hi] if hi is not None else pd.Series([], dtype=float)

            if not out_lo.empty or not out_hi.empty:
                bounds = f"[{lo}, {hi if hi is not None else '∞'}]"
                warnings.append(
                    f"[C] {col} out of bounds {bounds} ({note}). "
                    f"min={series.min():.4f}, max={series.max():.4f}."
                )

    # ------------------------------------------------------------------
    # D: Segment MRR sum ≈ total MRR
    # Build a month → total_mrr lookup from mrr_df (month is a date object).
    # ------------------------------------------------------------------
    if not seg_df.empty and not mrr_df.empty:
        total_mrr_by_month = mrr_df.set_index("month")["mrr"]

        for seg_type in ["plan", "region", "channel"]:
            subset = seg_df[seg_df["segment_type"] == seg_type]
            if subset.empty:
                continue

            seg_monthly = (
                subset.groupby("month")["mrr"]
                .sum()
                .reindex(total_mrr_by_month.index, fill_value=0.0)
            )

            denom_d = total_mrr_by_month.replace(0, np.nan)
            frac_diff = ((total_mrr_by_month - seg_monthly) / denom_d).dropna()

            max_gap = frac_diff.max()
            if max_gap > seg_tol:
                warnings.append(
                    f"[D] Segment '{seg_type}' MRR sum is up to {max_gap:.1%} below total MRR "
                    f"in some months. Expected if many customers have null '{seg_type}' values "
                    f"(they are excluded from segment rows)."
                )

    # ------------------------------------------------------------------
    # E: Negative raw MRR balances
    # Detects over-contracted or over-refunded customers. These are clamped
    # to 0 in reporting_mrr, so KPI outputs are not affected, but the raw
    # data may need investigation (e.g. credits issued before matching charges,
    # or incorrect event amounts in the source CSV).
    # ------------------------------------------------------------------
    if mrr_timeline is not None:
        neg = mrr_timeline[mrr_timeline["mrr_balance"] < 0]
        if not neg.empty:
            count = len(neg)
            worst_idx = neg["mrr_balance"].idxmin()
            min_balance = neg.loc[worst_idx, "mrr_balance"]
            worst_month = neg.loc[worst_idx, "month"]
            worst_customer = neg.loc[worst_idx, "customer_id"]
            warnings.append(
                f"[E] {count} customer-month row(s) have a negative raw MRR balance "
                f"(reported as 0 via reporting_mrr floor). "
                f"Worst: customer '{worst_customer}' at {min_balance:.2f} in {worst_month}. "
                f"Check for over-refund or mis-signed contraction/churn event amounts."
            )

    # ------------------------------------------------------------------
    # F: Event sign convention
    # new/expansion events should have positive amounts;
    # contraction/churn events should have negative amounts.
    # Checks raw event rows directly; warns with violation counts per type.
    # ------------------------------------------------------------------
    if raw_df is not None and not raw_df.empty:
        sign_rules = {
            "new": raw_df["event_type"] == "new",
            "expansion": raw_df["event_type"] == "expansion",
            "contraction": raw_df["event_type"] == "contraction",
            "churn": raw_df["event_type"] == "churn",
        }
        should_be_positive = {"new", "expansion"}
        sign_violations: dict[str, int] = {}

        for et, mask in sign_rules.items():
            subset = raw_df.loc[mask, "amount"]
            if subset.empty:
                continue
            if et in should_be_positive:
                bad = int((subset < 0).sum())
            else:
                bad = int((subset > 0).sum())
            if bad:
                sign_violations[et] = bad

        if sign_violations:
            detail = ", ".join(
                f"{et}: {n} row(s)" for et, n in sign_violations.items()
            )
            warnings.append(
                f"[F] Event sign convention violated. "
                f"new/expansion should be positive; contraction/churn should be negative. "
                f"Violations by event_type: {detail}."
            )

    return warnings


# ---------------------------------------------------------------------------
# Upsert helpers (delete + bulk insert)
# ---------------------------------------------------------------------------


def _to_none(val) -> Optional[float]:
    """Convert NaN/inf to None for SQLAlchemy nullable columns."""
    if val is None:
        return None
    try:
        if np.isnan(val) or np.isinf(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def _upsert_mrr(dataset_id: str, df: pd.DataFrame, db: Session) -> int:
    """
    Replace kpi_mrr_monthly rows for dataset_id with fresh computed values.

    Parameters
    ----------
    dataset_id : UUID string of the parent Dataset.
    df         : Output of compute_monthly_mrr_components().
    db         : Active SQLAlchemy Session (not yet committed).

    Returns
    -------
    Number of rows inserted.
    """
    db.query(models.KpiMrrMonthly).filter_by(dataset_id=dataset_id).delete(
        synchronize_session=False
    )

    records = [
        {
            "dataset_id": dataset_id,
            "month": row["month"],
            "mrr": _to_none(row["mrr"]),
            "new_mrr": _to_none(row["new_mrr"]),
            "expansion_mrr": _to_none(row["expansion_mrr"]),
            "contraction_mrr": _to_none(row["contraction_mrr"]),
            "churn_mrr": _to_none(row["churn_mrr"]),
            "net_new_mrr": _to_none(row["net_new_mrr"]),
        }
        for row in df.to_dict("records")
    ]

    if records:
        db.bulk_insert_mappings(models.KpiMrrMonthly, records)

    return len(records)


def _upsert_churn(dataset_id: str, df: pd.DataFrame, db: Session) -> int:
    """
    Replace kpi_churn_monthly rows for dataset_id with fresh computed values.
    """
    db.query(models.KpiChurnMonthly).filter_by(dataset_id=dataset_id).delete(
        synchronize_session=False
    )

    records = [
        {
            "dataset_id": dataset_id,
            "month": row["month"],
            "customer_churn_rate": _to_none(row["customer_churn_rate"]),
            "revenue_churn_rate": _to_none(row["revenue_churn_rate"]),
            "grr": _to_none(row["grr"]),
            "nrr": _to_none(row["nrr"]),
        }
        for row in df.to_dict("records")
    ]

    if records:
        db.bulk_insert_mappings(models.KpiChurnMonthly, records)

    return len(records)


def _upsert_segments(dataset_id: str, df: pd.DataFrame, db: Session) -> int:
    """
    Replace kpi_segments_monthly rows for dataset_id with fresh computed values.
    """
    db.query(models.KpiSegmentsMonthly).filter_by(dataset_id=dataset_id).delete(
        synchronize_session=False
    )

    records = [
        {
            "dataset_id": dataset_id,
            "month": row["month"],
            "segment_type": row["segment_type"],
            "segment_value": row["segment_value"],
            "mrr": _to_none(row["mrr"]),
            "churn_rate": _to_none(row["churn_rate"]),
            "mrr_at_risk": _to_none(row["mrr_at_risk"]),
        }
        for row in df.to_dict("records")
    ]

    if records:
        db.bulk_insert_mappings(models.KpiSegmentsMonthly, records)

    return len(records)


def _upsert_cohorts(dataset_id: str, df: pd.DataFrame, db: Session) -> int:
    """
    Replace cohort_retention rows for dataset_id with fresh computed values.
    """
    db.query(models.CohortRetention).filter_by(dataset_id=dataset_id).delete(
        synchronize_session=False
    )

    records = [
        {
            "dataset_id": dataset_id,
            "cohort_month": row["cohort_month"],
            "age_month": int(row["age_month"]),
            "retained_pct": _to_none(row["retained_pct"]),
            "revenue_retained_pct": _to_none(row["revenue_retained_pct"]),
        }
        for row in df.to_dict("records")
    ]

    if records:
        db.bulk_insert_mappings(models.CohortRetention, records)

    return len(records)
