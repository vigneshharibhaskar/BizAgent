# BizAgent Backend

AI Business Analyst Agent backend for SaaS revenue analytics.
Built with FastAPI, SQLAlchemy, Pandas, and LangGraph.

## Architecture

```
backend/
├── app/
│   ├── main.py                    # FastAPI app, lifespan, CORS, routers
│   ├── core/config.py             # Pydantic Settings, env loading
│   ├── db/
│   │   ├── session.py             # Engine, SessionLocal, get_db
│   │   └── models.py              # Dataset + RevenueEvent ORM models
│   ├── schemas/dataset.py         # Pydantic request/response schemas
│   ├── api/routes/upload.py       # /datasets/* HTTP endpoints
│   └── services/
│       └── dataset_loader.py      # CSV ETL service (no HTTP knowledge)
├── uploads/                       # Persisted CSV files (git-ignored)
├── requirements.txt
└── README.md
```

Data flow for a CSV upload:

```
POST /datasets/upload
  → upload.py: validate extension, save file to disk
  → dataset_loader.load_dataset(): read CSV → validate → transform → insert
      → validate_schema()          check required columns (case-insensitive)
      → validate_event_types()     check domain values
      → transform_rows()           parse dates, cast floats, fill optional cols
      → insert_dataset()           flush Dataset row (no commit yet)
      → insert_revenue_events()    bulk_insert_mappings for all event rows
      → db.commit()                atomic: all-or-nothing
  → UploadResponse JSON (HTTP 201)
```

## Setup

### 1. Enter the backend directory

```bash
cd backend
```

### 2. Create and activate a virtual environment

```bash
python3.11 -m venv venv
source venv/bin/activate        # macOS/Linux
# venv\Scripts\activate         # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment (optional)

```bash
cp .env.example .env
```

Default values work for local development without changes.

| Variable     | Default                 | Description                   |
|--------------|-------------------------|-------------------------------|
| DATABASE_URL | sqlite:///./bizagent.db | SQLAlchemy connection string  |
| UPLOAD_DIR   | ./uploads               | Directory for saved CSV files |
| APP_NAME     | BizAgent                | Displayed in API docs         |
| APP_VERSION  | 0.1.0                   | Displayed in /health response |

### 5. Run the development server

```bash
uvicorn app.main:app --reload --port 8000
```

- API: http://localhost:8000
- Interactive docs: http://localhost:8000/docs
- The SQLite database (`bizagent.db`) is created automatically on first startup.

## API Endpoints

### Step 1 — Dataset ingestion

| Method | Path                      | Description                            |
|--------|---------------------------|----------------------------------------|
| GET    | /health                   | Liveness probe                         |
| POST   | /datasets/upload          | Upload a CSV and ingest revenue events |
| GET    | /datasets/                | List all datasets (newest first)       |
| GET    | /datasets/{dataset_id}    | Get a single dataset by UUID           |

### Step 2 — KPI computation

| Method | Path                                    | Description                                         |
|--------|-----------------------------------------|-----------------------------------------------------|
| POST   | /datasets/{dataset_id}/kpis/run         | Compute and store all KPIs (idempotent)             |
| GET    | /datasets/{dataset_id}/kpis/mrr         | Monthly MRR + growth components                    |
| GET    | /datasets/{dataset_id}/kpis/churn       | Monthly churn rate, GRR, NRR                       |
| GET    | /datasets/{dataset_id}/kpis/segments    | MRR + churn by plan / region / channel per month   |
| GET    | /datasets/{dataset_id}/kpis/cohorts     | Cohort retention curves                            |

### POST /datasets/upload

Form fields:

| Field | Type        | Required | Description                        |
|-------|-------------|----------|------------------------------------|
| file  | File (.csv) | Yes      | CSV file containing revenue events |
| name  | String      | Yes      | Human-readable label               |

Returns HTTP 201 with an `UploadResponse` body on success.

```json
{
  "dataset": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "Q1 2024",
    "uploaded_at": "2024-03-01T10:00:00",
    "row_count": 4
  },
  "events_loaded": 4,
  "message": "Successfully loaded 4 revenue events for dataset 'Q1 2024'."
}
```

## CSV Format

The uploaded CSV must have a header row with at minimum the following columns
(column names are case-insensitive):

| Column      | Type       | Required | Allowed Values                         |
|-------------|------------|----------|----------------------------------------|
| event_date  | YYYY-MM-DD | Yes      | Any valid date                         |
| customer_id | String     | Yes      | Opaque identifier                      |
| plan        | String     | Yes      | e.g. starter, pro, enterprise          |
| event_type  | String     | Yes      | new, expansion, contraction, churn     |
| amount      | Float      | Yes      | MRR/ARR delta (non-zero)               |
| signup_date | YYYY-MM-DD | Yes      | Customer's original signup date        |
| region      | String     | No       | e.g. EMEA, APAC, Americas              |
| channel     | String     | No       | e.g. organic, paid                     |

### Example CSV

```csv
event_date,customer_id,plan,event_type,amount,signup_date,region,channel
2024-01-15,cust_001,pro,new,499.00,2024-01-15,EMEA,organic
2024-01-20,cust_002,enterprise,new,1999.00,2024-01-20,Americas,paid
2024-02-01,cust_001,enterprise,expansion,1500.00,2024-01-15,EMEA,organic
2024-03-10,cust_003,starter,churn,-99.00,2023-11-01,APAC,organic
```

## Step 2: Running KPI Computation

After uploading a CSV, trigger KPI computation with:

```bash
curl -X POST http://localhost:8000/datasets/{dataset_id}/kpis/run
```

Example response (HTTP 202):

```json
{
  "dataset_id": "550e8400-e29b-41d4-a716-446655440000",
  "months_computed": 6,
  "segments_computed": 48,
  "cohorts_computed": 18,
  "message": "KPIs computed for dataset 'Q1 2024': 6 months, 48 segment rows, 18 cohort data points."
}
```

Then query the results:

```bash
# Monthly MRR breakdown
curl http://localhost:8000/datasets/{dataset_id}/kpis/mrr

# Churn, GRR, NRR
curl http://localhost:8000/datasets/{dataset_id}/kpis/churn

# Segment breakdown (all dimensions)
curl http://localhost:8000/datasets/{dataset_id}/kpis/segments

# Filter to one segment dimension
curl "http://localhost:8000/datasets/{dataset_id}/kpis/segments?segment_type=plan"

# Cohort retention curves
curl http://localhost:8000/datasets/{dataset_id}/kpis/cohorts
```

**KPI Computation Rules:**

- `mrr` = sum of all positive customer MRR balances at end of month
- `contraction_mrr` / `churn_mrr` = stored as positive absolute values
- `net_new_mrr` = new + expansion − contraction − churn
- `grr` = (start_mrr − contraction − churn) / start_mrr
- `nrr` = (start_mrr + expansion − contraction − churn) / start_mrr
- Rate metrics are `null` when the prior month has zero customers or zero MRR
- The run endpoint is **idempotent** — re-running replaces prior results atomically

**New DB tables added in Step 2:**
`kpi_mrr_monthly`, `kpi_churn_monthly`, `kpi_segments_monthly`, `cohort_retention`
All cascade-delete when their parent dataset is deleted.

---

## Step 3 — Insights (AI-powered analysis)

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /datasets/{dataset_id}/insights/generate?month=YYYY-MM | Generate (or serve cached) AI insights for one month |
| GET | /datasets/{dataset_id}/insights/latest | Return the most recently generated insight |

### Usage

```bash
# 1. Run KPIs first (required — insights read from KPI tables)
curl -X POST http://localhost:8000/datasets/{dataset_id}/kpis/run

# 2. Generate insights for a specific month
curl -X POST "http://localhost:8000/datasets/{dataset_id}/insights/generate?month=2024-03"

# 3. Re-request the same month — served from cache instantly (cached=true)
curl -X POST "http://localhost:8000/datasets/{dataset_id}/insights/generate?month=2024-03"

# 4. Fetch the latest saved insight without triggering generation
curl http://localhost:8000/datasets/{dataset_id}/insights/latest
```

### How it works

```
POST /insights/generate
  → insight_engine.build_findings()        read KPI tables → compact Findings JSON
  → insight_engine.compute_digest_hash()   SHA-256(Findings + prompt_version)
  → llm_cache lookup                       cache hit → return immediately (cached=true)
  → ai_insights.generate_insights_from_findings()   cache miss → LLM call
      → validate AnalysisResponse schema
      → retry once with correction prompt on failure
  → store in llm_cache + insights tables
  → return InsightGenerateResponse
```

**Key guarantees:**

- The LLM **never sees raw revenue_events rows** — only a compact Findings payload
  built from pre-computed KPI aggregates (< 8 KB).
- **No PII is sent to the LLM.** Findings contains only numeric aggregates
  (MRR totals, rates, segment sums). No customer emails, names, or identifiers
  are included at any point in the pipeline.
- Caching is **content-addressed**: identical Findings + prompt version → same
  SHA-256 hash → same cached response, no redundant LLM calls.
- Each saved snapshot stores the `prompt_version` used, so the full chain
  (Findings + prompt → analysis) is deterministically reproducible.
- Changing the system prompt: increment `_PROMPT_VERSION` in
  `api/routes/insights.py` to invalidate old cache entries automatically.
- **Stub mode**: if `OPENAI_API_KEY` is not set, a clearly-labelled deterministic
  stub response is returned — the full pipeline still works end-to-end.
- **Network failures** from the AI provider return HTTP 503 (not 500), so
  callers can distinguish transient outages from application bugs.

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| OPENAI_API_KEY | *(empty)* | OpenAI API key. Leave empty for stub mode. |
| INSIGHTS_MODEL | `gpt-4o-mini` | Model used for insight generation. |

Add to `.env`:
```
OPENAI_API_KEY=sk-...
INSIGHTS_MODEL=gpt-4o-mini
```

### Example response (HTTP 200)

```json
{
  "dataset_id": "550e8400-e29b-41d4-a716-446655440000",
  "month": "2024-03",
  "digest_hash": "a3f9c2...",
  "cached": false,
  "analysis": {
    "type": "insight_batch",
    "title": "March 2024 — Strong Expansion Despite Elevated Enterprise Churn",
    "summary_bullets": [
      "MRR grew 11% MoM to $50,000, driven by $8,000 in new and $2,000 in expansion MRR.",
      "Customer churn rate rose 1pp to 5%, with enterprise segment contributing the highest MRR-at-risk.",
      "NRR of 1.10 confirms net expansion is outpacing revenue churn, supporting healthy growth momentum."
    ],
    "prioritized_actions": [
      {"priority": 1, "title": "Investigate enterprise churn spike", "rationale": "...", "expected_impact": "...", "confidence": 0.85},
      {"priority": 2, "title": "Accelerate expansion in EMEA", "rationale": "...", "expected_impact": "...", "confidence": 0.75},
      {"priority": 3, "title": "Review starter plan retention", "rationale": "...", "expected_impact": "...", "confidence": 0.70}
    ],
    "next_checks": [
      "Drill into individual enterprise accounts that churned in March.",
      "Compare EMEA expansion MRR trend over the last 3 months.",
      "Check M3 cohort retention for starter plan cohorts."
    ],
    "key_numbers": {"mrr": 50000, "net_new_mrr": 5000, "nrr": 1.10, "customer_churn_rate": 0.05},
    "assumptions": [],
    "confidence": 0.82
  }
}
```

**New DB tables added in Step 3:**
`llm_cache`, `insights`
`insights` cascade-deletes when its parent dataset is deleted.
`llm_cache` is keyed by content hash and is shared across datasets.

---

## Development Notes

- Delete `bizagent.db` and restart the server to reset the database.
  **Required after Step 3** — new columns (`datasets.latest_kpi_warnings`) and
  tables (`llm_cache`, `insights`) are only created on a fresh DB.
- Add `uploads/` and `bizagent.db` to `.gitignore`.
- The Ask Agent (Step 5) uses LangGraph 0.2.x. If you upgraded from an older venv, run `pip install -r requirements.txt` again.
- To switch to PostgreSQL: update `DATABASE_URL` in `.env` and remove `connect_args` from `db/session.py`.

---

## Step 5 — Ask Agent

### Overview

`POST /datasets/{dataset_id}/ask` runs a multi-step LangGraph agent that answers
natural-language questions about a dataset using only pre-computed KPI aggregates.
**No raw `revenue_events` rows are ever sent to the LLM.**

```
Architecture (max 2 tool loops):

  START → planner (LLM #1) → executor (tools) → reflect (LLM #2)
                                  ↑                     ↓ needs_followup?
                                  └──── executor ────────┘
                                        (LLM #3 in reflect, forced final)
```

### New files

| File | Purpose |
|------|---------|
| `app/schemas/scenario.py` | `ScenarioSpec` — describes a hypothetical KPI change |
| `app/schemas/ask.py` | `AskRequest` + `AskResponse` |
| `app/services/ask_tools.py` | Deterministic DB tools (no LLM): `get_headline`, `get_top_drivers`, `get_cohort_points`, `run_scenario`, `build_compact_context` |
| `app/agent/ask_graph.py` | LangGraph `StateGraph` with planner → executor → reflect nodes |
| `app/api/routes/ask.py` | FastAPI route; month auto-detected from latest KPI data |

### Endpoint

```
POST /datasets/{dataset_id}/ask
Content-Type: application/json

{
  "query": "Why did NRR drop in March 2024?",
  "month": "2024-03",     // optional — auto-detected if omitted
  "debug": false           // set true for agent trace in response
}
```

### Example — explain a metric drop

```bash
curl -s -X POST http://localhost:8000/datasets/<id>/ask \
  -H "Content-Type: application/json" \
  -d '{"query": "Why did NRR drop in March 2024?", "month": "2024-03"}' \
  | python3 -m json.tool
```

Response shape (same `AnalysisResponse` schema as `/insights/generate`):

```json
{
  "dataset_id": "550e8400-...",
  "query": "Why did NRR drop in March 2024?",
  "month": "2024-03",
  "analysis": {
    "type": "insight_batch",
    "title": "NRR Decline Analysis — March 2024",
    "summary_bullets": ["...", "...", "..."],
    "prioritized_actions": [
      {"priority": 1, "title": "...", "rationale": "...", "expected_impact": "...", "confidence": 0.85},
      ...
    ],
    "next_checks": ["...", "...", "..."],
    "key_numbers": {"mrr": 125000, "nrr": 0.94, "customer_churn_rate": 0.06},
    "assumptions": ["..."],
    "confidence": 0.82
  }
}
```

### Example — scenario / forecast

```bash
curl -s -X POST http://localhost:8000/datasets/<id>/ask \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What happens to ARR if we cut churn by 2 pp over 6 months?",
    "debug": true
  }' | python3 -m json.tool
```

With `debug: true`, the response includes an `AgentTrace` object:

```json
{
  "trace": {
    "agent_plan": ["get_headline", "get_top_drivers", "run_scenario"],
    "tool_calls": ["get_headline", "get_top_drivers", "run_scenario"],
    "scenario_run": true,
    "iterations": 2,
    "model": "gpt-4o-mini",
    "cached": false
  }
}
```

`agent_plan` lists tools the planner intended to call; `tool_calls` lists tools that were actually executed (may differ if reflect requested an extra tool on loop 2).

### Response type enum

The `analysis.type` field acts as a discriminator for the agent's intent:

| Value | Meaning |
|-------|---------|
| `"insight_batch"` | Strategic recommendations — the default for most queries |
| `"explanation"` | Root-cause analysis — used when the query asks "why" |
| `"forecast"` | Scenario / projection — used when `run_scenario` was called |

### Stub mode

If `OPENAI_API_KEY` is not configured, the agent returns a deterministic stub
`AnalysisResponse` with `"[STUB]"` in the title. All KPI tool calls still run,
so `key_numbers` contains real data. Set `OPENAI_API_KEY` in `.env` to enable
real AI analysis.

### Tools (deterministic — no LLM)

| Tool | Source tables | Output |
|------|--------------|--------|
| `get_headline` | `kpi_mrr_monthly`, `kpi_churn_monthly` | MRR, NRR, GRR, churn rates + deltas |
| `get_top_drivers` | `kpi_segments_monthly` | Top 5 churn-risk + MRR-decline segments |
| `get_cohort_points` | `cohort_retention` | 2 cohorts × age 0/1/3 retention |
| `run_scenario` | `kpi_mrr_monthly`, `kpi_churn_monthly` | MRR projection baseline vs scenario |
