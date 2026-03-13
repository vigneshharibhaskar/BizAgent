// ---------------------------------------------------------------------------
// UI state machine
// ---------------------------------------------------------------------------

export type UIState =
  | "idle"
  | "thinking"
  | "listening"
  | "transcribing"
  | "speaking";

// ---------------------------------------------------------------------------
// Backend API response shapes
// (Mirror the Pydantic schemas in backend/app/schemas/)
// ---------------------------------------------------------------------------

export interface DatasetResponse {
  id: string;
  name: string;
  uploaded_at: string;
  row_count: number | null;
}

// Matches backend/app/schemas/dataset.py :: UploadResponse
export interface UploadResponse {
  dataset: {
    id: string;
    name: string;
    uploaded_at: string;
    row_count: number | null;
  };
  events_loaded: number;
  message: string;
  warnings?: string[];
}

export interface KpiRunResponse {
  dataset_id: string;
  months_computed: number;
  segments_computed: number;
  cohorts_computed: number;
  message: string;
  warnings?: string[];
}

export interface MrrRow {
  month: string; // "YYYY-MM-DD" from backend Date field
  mrr: number | null;
  new_mrr: number | null;
  expansion_mrr: number | null;
  contraction_mrr: number | null;
  churn_mrr: number | null;
  net_new_mrr: number | null;
}

export interface ChurnRow {
  month: string;
  customer_churn_rate: number | null;
  revenue_churn_rate: number | null;
  grr: number | null;
  nrr: number | null;
}

export interface PrioritizedAction {
  priority: number;
  title: string;
  rationale: string;
  expected_impact: string;
  confidence: number;
}

export interface AnalysisResponse {
  type: string;
  title: string;
  summary_bullets: string[];
  prioritized_actions: PrioritizedAction[];
  next_checks: string[];
  key_numbers: Record<string, number | null>;
  assumptions: string[];
  confidence: number;
}

export interface InsightGenerateResponse {
  dataset_id: string;
  month: string; // "YYYY-MM"
  digest_hash: string;
  cached: boolean;
  analysis: AnalysisResponse;
}

// Matches backend/app/schemas/ask.py :: AgentTrace
export interface AgentTrace {
  agent_plan: string[];
  tool_calls: string[];
  scenario_run: boolean;
  iterations: number;
  model: string;
  cached: boolean;
}

// Matches backend/app/schemas/ask.py :: AskResponse
export interface AskResponse {
  dataset_id: string;
  query: string;
  month: string; // "YYYY-MM"
  analysis: AnalysisResponse;
  trace: AgentTrace | null;
}
