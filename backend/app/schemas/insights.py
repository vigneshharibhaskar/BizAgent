"""
insights.py
-----------
Pydantic response schemas for the insights API endpoints.
"""

from pydantic import BaseModel

from app.schemas.analysis_response import AnalysisResponse


class InsightGenerateResponse(BaseModel):
    """
    Response from POST /datasets/{dataset_id}/insights/generate.

    Fields
    ------
    dataset_id  : UUID of the dataset the insight covers.
    month       : Target period in YYYY-MM format.
    digest_hash : SHA-256 of (serialised Findings + prompt version).
                  Identical Findings always produce the same hash and
                  therefore hit the LLM cache.
    cached      : True when the AnalysisResponse was served from llm_cache
                  without a new LLM API call.
    analysis    : The structured AI insight report.
    """

    dataset_id: str
    month: str
    digest_hash: str
    cached: bool
    analysis: AnalysisResponse
