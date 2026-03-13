import type {
  UploadResponse,
  KpiRunResponse,
  MrrRow,
  ChurnRow,
  InsightGenerateResponse,
  AskResponse,
} from "./types";

const BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

// ---------------------------------------------------------------------------
// Generic fetch helper
// ---------------------------------------------------------------------------

async function apiFetch<T>(
  path: string,
  init?: RequestInit
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });

  if (!res.ok) {
    let detail: string;
    try {
      const body = await res.json();
      detail = body?.detail ?? res.statusText;
    } catch {
      detail = res.statusText;
    }
    throw new Error(`${res.status}: ${detail}`);
  }

  return res.json() as Promise<T>;
}

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

export async function uploadDataset(
  name: string,
  file: File
): Promise<UploadResponse> {
  const form = new FormData();
  form.append("name", name);
  form.append("file", file);

  const res = await fetch(`${BASE}/datasets/upload`, {
    method: "POST",
    body: form,
    // Let browser set multipart/form-data Content-Type with boundary
  });

  if (!res.ok) {
    let detail: string;
    try {
      const body = await res.json();
      detail = body?.detail ?? res.statusText;
    } catch {
      detail = res.statusText;
    }
    throw new Error(`${res.status}: ${detail}`);
  }

  return res.json() as Promise<UploadResponse>;
}

// ---------------------------------------------------------------------------
// KPIs
// ---------------------------------------------------------------------------

export async function runKpis(datasetId: string): Promise<KpiRunResponse> {
  return apiFetch<KpiRunResponse>(`/datasets/${datasetId}/kpis/run`, {
    method: "POST",
  });
}

export async function getMrr(datasetId: string): Promise<MrrRow[]> {
  return apiFetch<MrrRow[]>(`/datasets/${datasetId}/kpis/mrr`);
}

export async function getChurn(datasetId: string): Promise<ChurnRow[]> {
  return apiFetch<ChurnRow[]>(`/datasets/${datasetId}/kpis/churn`);
}

// ---------------------------------------------------------------------------
// Insights
// ---------------------------------------------------------------------------

export async function generateInsights(
  datasetId: string,
  month: string // "YYYY-MM"
): Promise<InsightGenerateResponse> {
  return apiFetch<InsightGenerateResponse>(
    `/datasets/${datasetId}/insights/generate?month=${encodeURIComponent(month)}`,
    { method: "POST" }
  );
}

export async function getLatestInsights(
  datasetId: string
): Promise<InsightGenerateResponse> {
  return apiFetch<InsightGenerateResponse>(
    `/datasets/${datasetId}/insights/latest`
  );
}

// ---------------------------------------------------------------------------
// Ask Agent
// ---------------------------------------------------------------------------

export async function askAgent(
  datasetId: string,
  query: string,
  month?: string,
  debug?: boolean
): Promise<AskResponse> {
  return apiFetch<AskResponse>(`/datasets/${datasetId}/ask`, {
    method: "POST",
    body: JSON.stringify({ query, month, debug }),
  });
}
