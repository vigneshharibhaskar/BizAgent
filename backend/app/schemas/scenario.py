"""
scenario.py
-----------
Pydantic schema for Ask Agent scenario simulation requests.

A ScenarioSpec describes a hypothetical change to one KPI (typically churn
or new_mrr) and asks the agent to project the impact over a future horizon.

Usage
-----
    spec = ScenarioSpec(
        metric="churn",
        change_type="absolute_pp",
        value=-2.0,      # reduce churn by 2 percentage points
        horizon_months=6,
    )
    result = ask_tools.run_scenario(dataset_id, spec, db)
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class ScenarioSpec(BaseModel):
    """
    Specification for a deterministic KPI scenario simulation.

    Fields
    ------
    metric          : KPI to modify. Currently supported: 'churn', 'new_mrr'.
    change_type     : How value is applied.
                      'absolute_pp'  — add value (in percentage points) to the rate.
                                       e.g. value=-2.0 means churn drops 2 pp.
                      'relative_pct' — multiply rate by (1 + value/100).
                                       e.g. value=-10.0 means churn drops 10 %.
    value           : Magnitude. Negative = improvement, positive = worsening.
    horizon_months  : Number of months to project forward (1–24).
    segment         : Optional segment filter, e.g. {"plan": "pro"}. Reserved
                      for future use — currently informational only.
    """

    metric: str = Field(default="churn", description="KPI to modify ('churn' or 'new_mrr')")
    change_type: Literal["absolute_pp", "relative_pct"]
    value: float = Field(
        description=(
            "Magnitude of change. For 'absolute_pp': percentage points added to the rate. "
            "For 'relative_pct': percent multiplier applied to the rate."
        )
    )
    horizon_months: int = Field(default=6, ge=1, le=24, description="Projection horizon in months")
    segment: Optional[dict] = Field(default=None, description="Optional segment filter (reserved)")
