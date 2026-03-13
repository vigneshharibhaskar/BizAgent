# BizAgent

AI-powered SaaS analytics platform. Upload a CSV of revenue events, compute KPIs, and ask natural-language questions answered by a LangGraph agent backed by GPT-4o.

---

## Architecture

```
BizAgent/
├── backend/          FastAPI + SQLAlchemy + LangGraph
└── frontend/         Next.js 15 + TypeScript + Tailwind CSS
```

---

## Features

- **Dataset upload** — CSV of customer revenue events ingested and validated
- **KPI engine** — MRR waterfall, NRR/GRR/churn rates, cohort retention, segment breakdowns computed into SQLite tables
- **AI Insights** — Monthly insight digest via OpenAI (cached by content hash)
- **Ask Agent** — `POST /datasets/{id}/ask` runs a LangGraph `planner → executor → reflect` graph; answers any question about your data using only pre-computed KPI aggregates (no raw events to the LLM)
- **Agent Trace** — Structured debug output: planner steps, tools called, iterations, model, scenario flag
- **Stub mode** — All endpoints work without an OpenAI API key; stub responses are clearly labelled

---

## Quick start

### Backend

```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # add OPENAI_API_KEY (optional)
uvicorn app.main:app --reload
# → http://localhost:8000
# → http://localhost:8000/docs
```

### Frontend

```bash
cd frontend
npm install

cp .env.local.example .env.local   # set NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
npm run dev
# → http://localhost:3000
```

---

## Backend endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/datasets/upload` | Upload CSV, returns `dataset_id` |
| `POST` | `/datasets/{id}/kpis/run` | Compute all KPI tables |
| `GET`  | `/datasets/{id}/kpis/mrr` | MRR waterfall by month |
| `GET`  | `/datasets/{id}/kpis/churn` | NRR / GRR / churn rates by month |
| `GET`  | `/datasets/{id}/kpis/segments` | Segment breakdown |
| `POST` | `/datasets/{id}/insights/generate` | Generate AI insight digest for a month |
| `GET`  | `/datasets/{id}/insights/latest` | Fetch latest cached digest |
| `POST` | `/datasets/{id}/ask` | Ask Agent — natural-language Q&A |

### Ask Agent request

```json
{
  "query": "Why did NRR drop in March?",
  "month": "2024-03",
  "debug": false
}
```

Set `debug: true` to receive an `AgentTrace` in the response:

```json
{
  "trace": {
    "agent_plan": ["get_headline", "get_top_drivers"],
    "tool_calls": ["get_headline", "get_top_drivers"],
    "scenario_run": false,
    "iterations": 1,
    "model": "gpt-4o-mini",
    "cached": false
  }
}
```

### Response type enum

| `analysis.type` | Meaning |
|-----------------|---------|
| `insight_batch` | Strategic recommendations (default) |
| `explanation`   | Root-cause analysis ("why" queries) |
| `forecast`      | Scenario / MRR projection |

---

## CSV format

The upload endpoint expects a CSV with these columns (extra columns ignored):

| Column | Type | Notes |
|--------|------|-------|
| `customer_id` | string | Unique customer identifier |
| `event_type` | string | `new`, `expansion`, `contraction`, `churn`, `reactivation` |
| `mrr` | number | Monthly recurring revenue in USD |
| `event_date` | date | `YYYY-MM-DD` |
| `plan` | string | Optional — used for segment breakdowns |
| `region` | string | Optional |
| `channel` | string | Optional |

---

## Environment variables

### Backend (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:///./bizagent.db` | SQLAlchemy connection string |
| `OPENAI_API_KEY` | — | Required for AI features; omit for stub mode |
| `INSIGHTS_MODEL` | `gpt-4o-mini` | Model used for insights and Ask Agent |
| `UPLOAD_DIR` | `./uploads` | Where uploaded CSVs are stored |

### Frontend (`.env.local`)

| Variable | Default | Description |
|----------|---------|-------------|
| `NEXT_PUBLIC_API_BASE_URL` | `http://localhost:8000` | Backend base URL |

---

## Agent architecture

```
POST /ask
  │
  ▼
planner  (LLM #1, T=0.1)
  │  JSON plan: intent + ordered tool list
  ▼
executor  (deterministic — no LLM)
  │  Runs tools: get_headline / get_top_drivers / get_cohort_points / run_scenario
  ▼
reflect  (LLM #2, T=0.2)
  │  Synthesises AnalysisResponse JSON
  │  May request one extra tool call (forecast queries only)
  └─► executor (at most once) ──► reflect (LLM #3, forced final)
```

- Max 2 tool-execution loops enforced by the graph router
- No raw `revenue_events` rows ever enter the LLM context
- Stub mode returns deterministic responses with real KPI numbers when `OPENAI_API_KEY` is absent

---

## Tech stack

| Layer | Technology |
|-------|-----------|
| Backend API | FastAPI 0.111, Pydantic v2 |
| ORM / DB | SQLAlchemy 2, SQLite (swap to Postgres via `DATABASE_URL`) |
| AI / Agent | OpenAI SDK, LangGraph 0.2.x |
| Frontend | Next.js 15, React 18, TypeScript |
| Styling | Tailwind CSS v3, CVA |
| Charts | Recharts 2 |
| Icons | lucide-react |
