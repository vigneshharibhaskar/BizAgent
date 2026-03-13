"""
ai_insights.py
--------------
AI insight generation service for Step 3 of BizAgent.

Responsibilities
----------------
- Accept a compact Findings payload (never raw event rows).
- Call the configured LLM to transform Findings into a structured
  AnalysisResponse.
- Validate the LLM response against the AnalysisResponse Pydantic schema;
  retry once with a correction instruction on failure.
- Return a deterministic stub AnalysisResponse when OPENAI_API_KEY is absent
  so the full API pipeline works without an AI subscription.

This module has NO knowledge of HTTP or the database.
"""

from __future__ import annotations

import json
import logging

from pydantic import ValidationError

from app.core.config import settings
from app.schemas.analysis_response import AnalysisResponse, PrioritizedAction
from app.schemas.findings import Findings

logger = logging.getLogger(__name__)


class AIServiceUnavailableError(Exception):
    """
    Raised when the AI provider is reachable but temporarily unavailable
    (network error, timeout, rate limit, server error).

    Distinct from ValidationError (schema mismatch) so the route layer can
    return HTTP 503 for transient failures instead of HTTP 500.
    """

# ---------------------------------------------------------------------------
# Prompt constant — bump PROMPT_VERSION in routes/insights.py when changed
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an expert SaaS business analyst. You will receive a JSON object called \
"Findings" containing pre-computed aggregate KPI metrics for one calendar month \
of a SaaS business. Your task is to analyse those metrics and return a \
structured insight report.

CRITICAL RULES:
1. Respond with ONLY a valid JSON object — no markdown, no code fences, \
no explanatory text outside the JSON.
2. Your JSON must exactly match this structure:

{
  "type": "insight_batch",
  "title": "<concise title for the period, max 80 chars>",
  "summary_bullets": ["<bullet 1>", "<bullet 2>", "<bullet 3>"],
  "prioritized_actions": [
    {
      "priority": 1,
      "title": "<action title, max 60 chars>",
      "rationale": "<why this matters, grounded in the data>",
      "expected_impact": "<quantified where possible>",
      "confidence": <float 0.0 to 1.0>
    },
    { "priority": 2, ... },
    { "priority": 3, ... }
  ],
  "next_checks": ["<check 1>", "<check 2>", "<check 3>"],
  "key_numbers": { "<human label>": <float or null>, ... },
  "assumptions": ["<assumption or data caveat>", ...],
  "confidence": <float 0.0 to 1.0>
}

CONSTRAINTS:
- summary_bullets: EXACTLY 3 items. One sentence each. Cover: (1) revenue \
performance, (2) churn / retention health, (3) growth momentum.
- prioritized_actions: EXACTLY 3 items. priority values must be exactly 1, 2, \
and 3. Sort by urgency — priority 1 is the most urgent action.
- next_checks: EXACTLY 3 items. Actionable follow-up data checks or questions.
- key_numbers: Include 3–6 of the most decision-relevant metrics with clear \
human-readable labels. Use JSON null for unavailable values.
- assumptions: Include ALL strings from data_quality.warnings in the Findings. \
Also include any additional assumptions you are making about the data.
- confidence: Reflect data completeness. Reduce confidence when many warnings \
are present, when prior-month data is absent, or when segment coverage is low.
- Base ALL insights strictly on the Findings JSON. Do not invent, extrapolate, \
or reference data not present in the payload.
"""

_CORRECTION_PROMPT = """\
Your previous response did not match the required schema.

Error: {error}

Please respond again with ONLY a valid JSON object that exactly matches the \
required structure. Ensure:
- summary_bullets has EXACTLY 3 string items.
- prioritized_actions has EXACTLY 3 objects with priority values 1, 2, 3.
- next_checks has EXACTLY 3 string items.
- No markdown, no code fences, no text outside the JSON.
"""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_insights_from_findings(findings: Findings) -> AnalysisResponse:
    """
    Transform a Findings payload into a structured AnalysisResponse.

    Behaviour
    ---------
    - If OPENAI_API_KEY is not configured, returns a clearly-labelled
      deterministic stub so the full API pipeline works without AI access.
    - If the API key is present, calls the configured LLM model with a
      strict JSON-only system prompt.
    - Validates the response against AnalysisResponse; retries once with
      a correction instruction on schema mismatch.
    - If the second attempt also fails, the ValidationError propagates to
      the route layer which returns HTTP 500.

    Parameters
    ----------
    findings : Compact Findings payload from insight_engine.build_findings().

    Returns
    -------
    Validated AnalysisResponse ready for caching and API response.

    Raises
    ------
    AIServiceUnavailableError : Network error, timeout, rate limit, or server
                                error from the AI provider (→ HTTP 503).
    ValidationError           : Both LLM attempts returned an invalid schema
                                (→ HTTP 500).
    """
    if not settings.OPENAI_API_KEY:
        logger.info("OPENAI_API_KEY not set — returning stub AnalysisResponse.")
        return _stub_response(findings)

    try:
        from openai import OpenAI  # lazy import — only needed when key is set
    except ImportError:
        logger.warning("openai package not installed — returning stub.")
        return _stub_response(findings)

    client = OpenAI(api_key=settings.OPENAI_API_KEY)
    model = settings.INSIGHTS_MODEL
    findings_str = json.dumps(findings.model_dump(), sort_keys=True, default=str)

    messages: list[dict] = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": findings_str},
    ]

    # --- First attempt ---
    raw1 = _call_llm(client, model, messages)  # raises AIServiceUnavailableError on network failure
    try:
        return AnalysisResponse.model_validate_json(raw1)
    except (json.JSONDecodeError, ValidationError) as exc:
        logger.warning("LLM response failed schema validation; retrying. Error: %s", exc)

    # --- Retry with correction ---
    messages_retry = messages + [
        {"role": "assistant", "content": raw1},
        {
            "role": "user",
            "content": _CORRECTION_PROMPT.format(error=str(exc)),
        },
    ]
    raw2 = _call_llm(client, model, messages_retry, temperature=0.1)
    # ValidationError on second failure propagates → route returns HTTP 500
    return AnalysisResponse.model_validate_json(raw2)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _call_llm(
    client,
    model: str,
    messages: list[dict],
    temperature: float = 0.2,
) -> str:
    """
    Call the OpenAI chat completions API and return the raw response text.

    Raises
    ------
    AIServiceUnavailableError : For connection errors, timeouts, rate limits,
                                or 5xx server errors — all transient failures
                                that warrant a 503 response to the caller.
    """
    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=temperature,
        )
        return response.choices[0].message.content
    except Exception as exc:
        # Import lazily so the module still loads when openai is not installed.
        try:
            from openai import (
                APIConnectionError,
                APITimeoutError,
                RateLimitError,
                InternalServerError,
            )
            transient = (APIConnectionError, APITimeoutError, RateLimitError, InternalServerError)
        except ImportError:
            transient = ()

        if transient and isinstance(exc, transient):
            logger.warning("Transient AI provider error: %s", exc)
            raise AIServiceUnavailableError(str(exc)) from exc
        raise


def _stub_response(findings: Findings) -> AnalysisResponse:
    """
    Return a clearly-labelled deterministic AnalysisResponse.

    Used when OPENAI_API_KEY is absent or the openai package is not installed.
    The stub is functional (the full API pipeline works) but makes it obvious
    to the caller that AI analysis is disabled.
    """
    period = findings.period
    mrr = findings.headline.mrr
    nrr = findings.headline.nrr
    churn = findings.headline.customer_churn_rate

    mrr_str = f"{mrr:,.0f}" if mrr is not None else "N/A"

    return AnalysisResponse(
        type="insight_batch",
        title=f"[STUB] {period} — configure OPENAI_API_KEY to enable AI insights",
        summary_bullets=[
            f"MRR for {period} is {mrr_str}. Configure OPENAI_API_KEY for AI-generated revenue commentary.",
            "AI analysis is unavailable because no API key is configured in the environment.",
            "Set OPENAI_API_KEY (and optionally INSIGHTS_MODEL) in your .env file to activate real insights.",
        ],
        prioritized_actions=[
            PrioritizedAction(
                priority=1,
                title="Configure OPENAI_API_KEY",
                rationale="AI insight generation is disabled without a valid API key.",
                expected_impact="Unlocks structured AI analysis for every KPI run.",
                confidence=1.0,
            ),
            PrioritizedAction(
                priority=2,
                title="Review MRR components manually",
                rationale="new_mrr, expansion_mrr, contraction_mrr, and churn_mrr are available via /kpis/mrr.",
                expected_impact="Identify the primary driver of MRR movement this month.",
                confidence=0.9,
            ),
            PrioritizedAction(
                priority=3,
                title="Inspect high-churn segments",
                rationale="Segment MRR-at-risk data is available via /kpis/segments.",
                expected_impact="Surface the customer groups most at risk of churning.",
                confidence=0.8,
            ),
        ],
        next_checks=[
            "Review monthly MRR breakdown at GET /datasets/{id}/kpis/mrr",
            "Check segment churn rates at GET /datasets/{id}/kpis/segments",
            "Examine cohort retention curves at GET /datasets/{id}/kpis/cohorts",
        ],
        key_numbers={
            "mrr": mrr,
            "nrr": nrr,
            "customer_churn_rate": churn,
            "net_new_mrr": findings.headline.net_new_mrr,
        },
        assumptions=[
            "This is a deterministic stub response — no AI analysis was performed.",
            "Set OPENAI_API_KEY in your environment to enable real insights.",
        ]
        + findings.data_quality.warnings,
        confidence=0.0,
    )
