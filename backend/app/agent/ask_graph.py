"""
ask_graph.py
------------
LangGraph-powered Ask Agent for BizAgent Step 5.

Architecture
------------
The agent runs a StateGraph with at most 2 tool execution loops:

  START → planner → executor → reflect → (optional) executor → END
                                  ↑____________↓ (if needs_followup)

Node responsibilities
---------------------
planner  (LLM call #1)
    Given the user query and month, outputs a JSON plan:
    intent + ordered list of tools to call + optional ScenarioSpec.
    Uses a minimal, low-temperature prompt to keep token cost low.

executor  (deterministic, no LLM)
    Runs each tool in the plan sequentially.
    On iteration > 0, runs only the single tool returned by reflect.
    Stores all results in agent state; never modifies them.

reflect  (LLM call #2, or #3 on loop 2)
    Reads accumulated tool outputs and either:
      a) Produces the final AnalysisResponse JSON (most queries), or
      b) Requests ONE additional tool call (scenario/forecast queries only).
    On iteration >= 1 it is forced to produce the final answer.
    Validates the LLM response; retries once with a correction prompt.

Constraints enforced
--------------------
- max_iterations = 2
- No raw revenue_events rows ever enter the LLM context.
- All numeric data comes from KPI tables via ask_tools.py.
- Stub mode active when OPENAI_API_KEY is absent.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Optional, TypedDict

from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.core.config import settings
from app.schemas.analysis_response import AnalysisResponse, PrioritizedAction
from app.schemas.ask import AgentTrace
from app.schemas.scenario import ScenarioSpec
from app.services import ask_tools
from app.services.ai_insights import AIServiceUnavailableError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_PLANNER_SYSTEM = """\
You are a query planner for a SaaS analytics agent. Given a user question and \
target month, output a minimal JSON execution plan.

Available tools:
- get_headline   : Current + prior month MRR, NRR, GRR, churn rates. No args needed.
- get_top_drivers: Top 5 churn-risk and MRR-decline segments. No args needed.
- get_cohort_points: Cohort retention summary for 2 cohorts. No args needed.
- run_scenario   : Deterministic MRR/churn projection. Requires ScenarioSpec in args.

Return ONLY valid JSON with NO markdown or code fences:
{
  "intent": "explanation" | "insight_batch" | "forecast",
  "steps": [
    {"tool": "<tool_name>", "args": {}},
    ...
  ],
  "scenario": null,
  "needs_followup": false
}

Rules:
- For revenue/NRR/churn questions: include get_headline + get_top_drivers.
- For cohort/retention questions: add get_cohort_points.
- For forecast/scenario questions: include run_scenario with args as ScenarioSpec JSON:
  {"metric":"churn","change_type":"absolute_pp","value":<float>,"horizon_months":6}
- Maximum 3 steps total. get_headline/get_top_drivers/get_cohort_points take empty args {}.
- Set needs_followup=true ONLY if you expect to request run_scenario after seeing data.
"""

_REFLECT_SYSTEM = """\
You are an expert SaaS business analyst. Based on the user query and the tool \
output data provided, generate a structured AnalysisResponse.

CRITICAL RULES:
1. Respond with ONLY a valid JSON object — no markdown, no code fences.
2. Your JSON must exactly match this structure:

{
  "type": "insight_batch" | "explanation" | "forecast",
  "title": "<concise title for the period, max 80 chars>",
  "summary_bullets": ["<bullet 1>", "<bullet 2>", "<bullet 3>"],
  "prioritized_actions": [
    {"priority": 1, "title": "...", "rationale": "...", "expected_impact": "...", "confidence": 0.0},
    {"priority": 2, "title": "...", "rationale": "...", "expected_impact": "...", "confidence": 0.0},
    {"priority": 3, "title": "...", "rationale": "...", "expected_impact": "...", "confidence": 0.0}
  ],
  "next_checks": ["<check 1>", "<check 2>", "<check 3>"],
  "key_numbers": {"<human label>": <float or null>, ...},
  "assumptions": ["<assumption or caveat>"],
  "confidence": 0.0
}

CONSTRAINTS:
- summary_bullets: EXACTLY 3 items. Cover (1) revenue performance, \
(2) churn/retention health, (3) growth momentum.
- prioritized_actions: EXACTLY 3 items. priority values must be exactly 1, 2, 3.
- next_checks: EXACTLY 3 items.
- key_numbers: 3–6 most decision-relevant metrics.
- Base ALL insights strictly on the provided data. Do not invent figures.
"""

_REFLECT_FOLLOWUP_SUFFIX = """\

ADDITIONAL OPTION (first pass only):
If you need scenario simulation data to answer the query, instead of returning \
AnalysisResponse you may return:
{"_request_tool": {"tool": "run_scenario", "args": {<ScenarioSpec JSON>}}}

Only use this if the query specifically asks for a projection or 'what-if' and \
run_scenario has not yet been called.
"""

_CORRECTION_PROMPT = """\
Your previous response did not match the required schema.
Error: {error}
Respond again with ONLY a valid JSON AnalysisResponse object. Ensure:
- summary_bullets has EXACTLY 3 string items.
- prioritized_actions has EXACTLY 3 objects with priority 1, 2, 3.
- next_checks has EXACTLY 3 string items.
- No markdown, no code fences, no text outside the JSON.
"""


# ---------------------------------------------------------------------------
# Agent state
# ---------------------------------------------------------------------------


class AgentState(TypedDict):
    """Immutable-style state threaded through all LangGraph nodes."""

    query: str
    month: str
    plan: dict
    tool_results: dict          # accumulated outputs keyed by tool name
    iteration: int              # incremented by executor on each pass
    next_step: Optional[dict]   # single tool requested by reflect for round 2
    needs_followup: bool        # reflects whether executor should run again
    analysis: Optional[dict]   # final AnalysisResponse model_dump
    trace: dict                 # execution trace for debug mode


# ---------------------------------------------------------------------------
# LLM helper (mirrors ai_insights._call_llm but local to this module)
# ---------------------------------------------------------------------------


def _llm_call(client, model: str, messages: list[dict], temperature: float = 0.2) -> str:
    """
    Call the OpenAI chat completions API. Returns raw response text.

    Raises
    ------
    AIServiceUnavailableError : For transient network/rate-limit failures.
    """
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=temperature,
        )
        return resp.choices[0].message.content
    except Exception as exc:
        try:
            from openai import APIConnectionError, APITimeoutError, RateLimitError, InternalServerError
            transient = (APIConnectionError, APITimeoutError, RateLimitError, InternalServerError)
        except ImportError:
            transient = ()
        if transient and isinstance(exc, transient):
            raise AIServiceUnavailableError(str(exc)) from exc
        raise


def _parse_analysis(raw: str) -> AnalysisResponse:
    """Parse and validate raw JSON string as AnalysisResponse."""
    return AnalysisResponse.model_validate_json(raw)


# ---------------------------------------------------------------------------
# Stub mode (no API key)
# ---------------------------------------------------------------------------


def _stub_response(query: str, month: str, ctx: dict) -> AnalysisResponse:
    """
    Return a clearly-labelled deterministic stub when OPENAI_API_KEY is absent.

    Uses available tool outputs for actual numbers; all narrative text is
    placeholder text that makes clear AI analysis is disabled.
    """
    headline = ctx.get("headline", {})
    mrr = headline.get("mrr")
    nrr = headline.get("nrr")
    churn = headline.get("customer_churn_rate")
    mrr_str = f"{mrr:,.0f}" if mrr is not None else "N/A"

    return AnalysisResponse(
        type="insight_batch",
        title=f"[STUB] {month} — configure OPENAI_API_KEY to enable AI insights",
        summary_bullets=[
            f"MRR for {month} is {mrr_str}. Configure OPENAI_API_KEY for AI-generated revenue commentary.",
            "AI analysis is unavailable because no API key is configured in the environment.",
            "Set OPENAI_API_KEY (and optionally INSIGHTS_MODEL) in your .env file to activate real insights.",
        ],
        prioritized_actions=[
            PrioritizedAction(
                priority=1,
                title="Configure OPENAI_API_KEY",
                rationale="Ask Agent requires a valid OpenAI API key.",
                expected_impact="Unlocks structured AI analysis for any query.",
                confidence=1.0,
            ),
            PrioritizedAction(
                priority=2,
                title="Review headline metrics manually",
                rationale="Raw KPI data is available even without AI.",
                expected_impact="Identify top revenue and churn drivers this month.",
                confidence=0.9,
            ),
            PrioritizedAction(
                priority=3,
                title="Check segment breakdown",
                rationale="Segment data is computed and available in /kpis/segments.",
                expected_impact="Surface which plan/region/channel is driving churn.",
                confidence=0.8,
            ),
        ],
        next_checks=[
            f"GET /datasets/{{id}}/kpis/mrr — MRR breakdown for {month}",
            f"GET /datasets/{{id}}/kpis/churn — NRR/GRR/churn rates for {month}",
            f"GET /datasets/{{id}}/kpis/segments — segment churn breakdown for {month}",
        ],
        key_numbers={
            "mrr": mrr,
            "nrr": nrr,
            "customer_churn_rate": churn,
        },
        assumptions=[
            "This is a deterministic stub — no AI analysis was performed.",
            f"Original query: {query!r}",
            "Set OPENAI_API_KEY in your environment to enable real Ask Agent responses.",
        ],
        confidence=0.0,
    )


# ---------------------------------------------------------------------------
# LangGraph graph factory
# ---------------------------------------------------------------------------


def _build_graph(dataset_id: str, month_date: date, db: Session, client, model: str):
    """
    Build and compile a fresh LangGraph StateGraph for a single request.

    All nodes capture dataset_id, month_date, db, client, and model via
    closure so the graph state only needs to carry serialisable data.

    Parameters
    ----------
    dataset_id  : UUID string of the parent Dataset.
    month_date  : First day of the target month.
    db          : Request-scoped SQLAlchemy Session.
    client      : Initialised OpenAI client.
    model       : Model name to use for LLM calls.

    Returns
    -------
    Compiled LangGraph runnable.
    """
    from langgraph.graph import END, START, StateGraph

    # ------------------------------------------------------------------
    # Node: planner
    # ------------------------------------------------------------------

    def planner(state: AgentState) -> dict:
        """LLM call #1 — produce execution plan."""
        messages = [
            {"role": "system", "content": _PLANNER_SYSTEM},
            {
                "role": "user",
                "content": (
                    f"Query: {state['query']}\n"
                    f"Month: {state['month']}"
                ),
            },
        ]
        raw = _llm_call(client, model, messages, temperature=0.1)

        try:
            plan = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Planner returned invalid JSON; using default plan. raw=%r", raw[:200])
            plan = {
                "intent": "explanation",
                "steps": [
                    {"tool": "get_headline", "args": {}},
                    {"tool": "get_top_drivers", "args": {}},
                ],
                "scenario": None,
                "needs_followup": False,
            }

        trace = {**state["trace"], "plan": plan, "llm_calls": state["trace"]["llm_calls"] + 1}
        return {"plan": plan, "needs_followup": bool(plan.get("needs_followup")), "trace": trace}

    # ------------------------------------------------------------------
    # Node: executor
    # ------------------------------------------------------------------

    def executor(state: AgentState) -> dict:
        """Deterministic tool runner — no LLM calls."""
        # Round 2+: run a single tool requested by reflect
        if state["iteration"] > 0 and state["next_step"]:
            steps = [state["next_step"]]
        else:
            steps = state["plan"].get("steps", [])

        results = dict(state["tool_results"])
        tools_called = list(state["trace"].get("tools_called", []))
        scenario_run = state["trace"].get("scenario_run", False)

        for step in steps:
            tool_name = step.get("tool", "")
            args = step.get("args", {})
            try:
                result = _run_tool(tool_name, args, dataset_id, month_date, db)
            except Exception as exc:
                logger.warning("Tool %r raised: %s", tool_name, exc)
                result = {"error": str(exc)}

            results[tool_name] = result
            output_bytes = len(json.dumps(result, default=str).encode())
            tools_called.append({"tool": tool_name, "output_bytes": output_bytes})
            if tool_name == "run_scenario":
                scenario_run = True

        trace = {
            **state["trace"],
            "tools_called": tools_called,
            "scenario_run": scenario_run,
        }
        return {
            "tool_results": results,
            "iteration": state["iteration"] + 1,
            "next_step": None,
            "trace": trace,
        }

    # ------------------------------------------------------------------
    # Node: reflect
    # ------------------------------------------------------------------

    def reflect(state: AgentState) -> dict:
        """LLM call #2 (or #3) — synthesize analysis or request one more tool."""
        tool_data = json.dumps(state["tool_results"], default=str, indent=2)
        is_final_pass = state["iteration"] >= 2

        system_prompt = _REFLECT_SYSTEM
        if not is_final_pass:
            system_prompt = _REFLECT_SYSTEM + _REFLECT_FOLLOWUP_SUFFIX

        messages = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    f"User query: {state['query']}\n"
                    f"Period: {state['month']}\n\n"
                    f"Data:\n{tool_data}"
                ),
            },
        ]

        raw = _llm_call(client, model, messages, temperature=0.2)

        # Check for tool-request response (only on first pass)
        if not is_final_pass:
            try:
                parsed = json.loads(raw)
                if "_request_tool" in parsed:
                    step = parsed["_request_tool"]
                    trace = {
                        **state["trace"],
                        "llm_calls": state["trace"]["llm_calls"] + 1,
                    }
                    return {
                        "next_step": step,
                        "needs_followup": True,
                        "trace": trace,
                    }
            except json.JSONDecodeError:
                pass  # fall through to AnalysisResponse parse

        # Parse as AnalysisResponse
        try:
            analysis = _parse_analysis(raw)
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning("Reflect response failed validation; retrying. Error: %s", exc)
            retry_messages = messages + [
                {"role": "assistant", "content": raw},
                {"role": "user", "content": _CORRECTION_PROMPT.format(error=str(exc))},
            ]
            raw2 = _llm_call(client, model, retry_messages, temperature=0.1)
            analysis = _parse_analysis(raw2)  # ValidationError propagates → HTTP 500

        trace = {
            **state["trace"],
            "llm_calls": state["trace"]["llm_calls"] + 1,
            "needs_followup": False,
        }
        return {
            "analysis": analysis.model_dump(),
            "needs_followup": False,
            "next_step": None,
            "trace": trace,
        }

    # ------------------------------------------------------------------
    # Conditional router
    # ------------------------------------------------------------------

    def router(state: AgentState) -> str:
        """Route reflect output: run executor again or finish."""
        if state.get("needs_followup") and state["iteration"] < 2:
            return "executor"
        return END

    # ------------------------------------------------------------------
    # Graph assembly
    # ------------------------------------------------------------------

    g: StateGraph = StateGraph(AgentState)
    g.add_node("planner", planner)
    g.add_node("executor", executor)
    g.add_node("reflect", reflect)

    g.add_edge(START, "planner")
    g.add_edge("planner", "executor")
    g.add_edge("executor", "reflect")
    g.add_conditional_edges("reflect", router)

    return g.compile()


# ---------------------------------------------------------------------------
# Tool dispatcher (called by executor)
# ---------------------------------------------------------------------------


def _run_tool(tool_name: str, args: dict, dataset_id: str, month_date: date, db: Session) -> dict:
    """
    Dispatch a tool call to the corresponding ask_tools function.

    All tools receive dataset_id, month_date, and db from the closure.
    Only run_scenario requires additional args (parsed as ScenarioSpec).
    """
    if tool_name == "get_headline":
        return ask_tools.get_headline(dataset_id, month_date, db)
    if tool_name == "get_top_drivers":
        return ask_tools.get_top_drivers(dataset_id, month_date, db)
    if tool_name == "get_cohort_points":
        return ask_tools.get_cohort_points(dataset_id, db)
    if tool_name == "run_scenario":
        scenario = ScenarioSpec(**args)
        result = ask_tools.run_scenario(dataset_id, scenario, db)
        return result
    logger.warning("Unknown tool requested: %r — skipping.", tool_name)
    return {"error": f"Unknown tool: {tool_name!r}"}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_ask(
    dataset_id: str,
    query: str,
    month_date: date,
    db: Session,
    debug: bool = False,
) -> tuple[AnalysisResponse, Optional[AgentTrace]]:
    """
    Run the Ask Agent and return a structured analysis.

    Parameters
    ----------
    dataset_id  : UUID string of the parent Dataset.
    query       : Natural-language question from the user.
    month_date  : First day of the target analysis month.
    db          : Active request-scoped SQLAlchemy Session.
    debug       : If True, return a populated AgentTrace as the second element.

    Returns
    -------
    (AnalysisResponse, AgentTrace | None)

    Stub mode
    ---------
    When OPENAI_API_KEY is absent or the openai package is not installed,
    the function runs all deterministic tools to populate key_numbers, then
    returns a clearly-labelled stub AnalysisResponse without any LLM calls.

    Raises
    ------
    AIServiceUnavailableError : Transient OpenAI failure (→ HTTP 503).
    ValidationError           : Both LLM attempts produced invalid schema (→ HTTP 500).
    """
    month_str = month_date.strftime("%Y-%m")

    # --- Stub mode: no API key ---
    if not settings.OPENAI_API_KEY:
        logger.info("OPENAI_API_KEY not set — returning stub AskResponse.")
        ctx = ask_tools.build_compact_context(dataset_id, month_date, db)
        stub = _stub_response(query, month_str, ctx)
        trace = AgentTrace(
            agent_plan=[],
            tool_calls=list(ctx.keys()),
            scenario_run=False,
            iterations=0,
            model="",
            cached=False,
        ) if debug else None
        return stub, trace

    # --- Try importing openai ---
    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("openai package not installed — returning stub.")
        ctx = ask_tools.build_compact_context(dataset_id, month_date, db)
        stub = _stub_response(query, month_str, ctx)
        trace = AgentTrace(
            agent_plan=[],
            tool_calls=list(ctx.keys()),
            scenario_run=False,
            iterations=0,
            model="",
            cached=False,
        ) if debug else None
        return stub, trace

    # --- Try importing langgraph ---
    try:
        from langgraph.graph import StateGraph  # noqa: F401 — import check only
    except ImportError:
        logger.warning("langgraph package not installed — falling back to single-pass reflect.")
        return _fallback_single_pass(dataset_id, query, month_date, month_str, db, debug)

    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    model = settings.INSIGHTS_MODEL

    # --- Build and run graph ---
    graph = _build_graph(dataset_id, month_date, db, client, model)

    initial_state: AgentState = {
        "query": query,
        "month": month_str,
        "plan": {},
        "tool_results": {},
        "iteration": 0,
        "next_step": None,
        "needs_followup": False,
        "analysis": None,
        "trace": {
            "model": model,
            "llm_calls": 0,
            "tools_called": [],
            "scenario_run": False,
        },
    }

    final_state: AgentState = graph.invoke(initial_state)

    analysis = AnalysisResponse.model_validate(final_state["analysis"])

    trace: Optional[AgentTrace] = None
    if debug:
        raw_trace = final_state["trace"]
        plan_steps = raw_trace.get("plan", {}).get("steps", [])
        trace = AgentTrace(
            agent_plan=[s["tool"] for s in plan_steps if isinstance(s, dict)],
            tool_calls=[t["tool"] for t in raw_trace.get("tools_called", []) if isinstance(t, dict)],
            scenario_run=raw_trace.get("scenario_run", False),
            iterations=final_state["iteration"],
            model=raw_trace.get("model", model),
            cached=False,
        )

    return analysis, trace


def _fallback_single_pass(
    dataset_id: str,
    query: str,
    month_date: date,
    month_str: str,
    db: Session,
    debug: bool,
) -> tuple[AnalysisResponse, Optional[AgentTrace]]:
    """
    Single-pass fallback when LangGraph is unavailable.

    Runs headline + top_drivers tools, then calls the LLM once to synthesize.
    Used as a graceful degradation path; not the primary code path.
    """
    from openai import OpenAI

    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    model = settings.INSIGHTS_MODEL

    ctx = ask_tools.build_compact_context(dataset_id, month_date, db)
    data_str = json.dumps(ctx, default=str, indent=2)

    messages = [
        {"role": "system", "content": _REFLECT_SYSTEM},
        {
            "role": "user",
            "content": f"User query: {query}\nPeriod: {month_str}\n\nData:\n{data_str}",
        },
    ]

    raw = _llm_call(client, model, messages)
    try:
        analysis = _parse_analysis(raw)
    except (json.JSONDecodeError, ValidationError) as exc:
        retry = messages + [
            {"role": "assistant", "content": raw},
            {"role": "user", "content": _CORRECTION_PROMPT.format(error=str(exc))},
        ]
        raw2 = _llm_call(client, model, retry, temperature=0.1)
        analysis = _parse_analysis(raw2)

    trace = AgentTrace(
        agent_plan=["get_headline", "get_top_drivers", "get_cohort_points"],
        tool_calls=list(ctx.keys()),
        scenario_run=False,
        iterations=1,
        model=model,
        cached=False,
    ) if debug else None
    return analysis, trace
