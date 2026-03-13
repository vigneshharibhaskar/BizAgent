"""
ask.py
------
Pydantic schemas for POST /datasets/{dataset_id}/ask.

AskRequest  — inbound query parameters.
AskResponse — structured analysis + optional debug trace.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from app.schemas.analysis_response import AnalysisResponse


class AskRequest(BaseModel):
    """
    Inbound payload for the Ask Agent endpoint.

    Fields
    ------
    query  : Natural-language question about the dataset.
    month  : Target analysis month in YYYY-MM format.
             Defaults to the most recent month with KPI data if omitted.
    voice  : Reserved for future voice-mode streaming. Currently unused.
    debug  : When True, include an 'AgentTrace' object in the response.
    """

    query: str = Field(..., min_length=3, max_length=1000, description="Natural-language question")
    month: Optional[str] = Field(
        default=None,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="Target month YYYY-MM (defaults to latest with KPI data)",
    )
    voice: bool = Field(default=False, description="Reserved for future voice-mode streaming")
    debug: bool = Field(default=False, description="Include agent trace in response")


class AgentTrace(BaseModel):
    """
    Structured agent execution trace returned when debug=True.

    Fields
    ------
    agent_plan    : Ordered list of tool names the planner chose to run.
    tool_calls    : Ordered list of tool names actually executed (may include
                    a second-loop tool not in the original plan).
    scenario_run  : True if run_scenario was executed during this request.
    iterations    : Number of executor passes (1 = single loop, 2 = followup).
    model         : OpenAI model ID used for LLM calls.
    cached        : Reserved — True when the response came from a cache hit.
                    Currently always False (ask responses are not cached).
    """

    agent_plan: list[str]
    tool_calls: list[str]
    scenario_run: bool = False
    iterations: int
    model: str
    cached: bool = False


class AskResponse(BaseModel):
    """
    Structured response from the Ask Agent.

    Fields
    ------
    dataset_id : The dataset that was analysed.
    query      : Echo of the input query.
    month      : The month that was analysed (YYYY-MM).
    analysis   : Structured AnalysisResponse (same schema as /insights/generate).
    trace      : Agent execution trace. Present only when request.debug=True.
    """

    dataset_id: str
    query: str
    month: str
    analysis: AnalysisResponse
    trace: Optional[AgentTrace] = None
