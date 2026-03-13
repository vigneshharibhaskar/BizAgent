"use client";

import { useState, useCallback } from "react";
import { getMrr, getChurn } from "@/lib/api";
import { latestMonth } from "@/lib/date";
import { StatusPill } from "@/components/StatusPill";
import { ErrorBanner } from "@/components/ErrorBanner";
import { DatasetUploader } from "@/components/DatasetUploader";
import { KpiRunPanel } from "@/components/KpiRunPanel";
import { HeadlineCards } from "@/components/HeadlineCards";
import { MrrChart } from "@/components/MrrChart";
import { RetentionChart } from "@/components/RetentionChart";
import { InsightsPanel } from "@/components/InsightsPanel";
import { AskPanel } from "@/components/AskPanel";
import type { UIState, MrrRow, ChurnRow } from "@/lib/types";

export default function Home() {
  // ---------------------------------------------------------------------------
  // Core state
  // ---------------------------------------------------------------------------
  const [uiState, setUiState] = useState<UIState>("idle");
  const [error, setError] = useState<string | null>(null);

  const [datasetId, setDatasetId] = useState<string | null>(null);
  const [datasetName, setDatasetName] = useState<string>("");

  const [mrrRows, setMrrRows] = useState<MrrRow[]>([]);
  const [churnRows, setChurnRows] = useState<ChurnRow[]>([]);

  // ---------------------------------------------------------------------------
  // Handlers
  // ---------------------------------------------------------------------------
  const handleUploaded = useCallback((id: string, name: string) => {
    setDatasetId(id);
    setDatasetName(name);
    setMrrRows([]);
    setChurnRows([]);
    setError(null);
  }, []);

  const handleKpisReady = useCallback(async () => {
    if (!datasetId) return;
    setUiState("thinking");
    try {
      const [mrr, churn] = await Promise.all([
        getMrr(datasetId),
        getChurn(datasetId),
      ]);
      setMrrRows(mrr);
      setChurnRows(churn);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load KPI data");
    } finally {
      setUiState("idle");
    }
  }, [datasetId]);

  const handleError = useCallback((msg: string) => {
    setError(msg);
    setUiState("idle");
  }, []);

  const handleThinking = useCallback((v: boolean) => {
    setUiState(v ? "thinking" : "idle");
  }, []);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  const hasData = mrrRows.length > 0;
  const selectedMonth = latestMonth(mrrRows.map((r) => r.month)) ?? undefined;

  return (
    <div className="flex min-h-screen flex-col bg-background">
      {/* Top bar */}
      <header className="sticky top-0 z-10 flex items-center justify-between border-b border-border bg-background/80 px-6 py-3 backdrop-blur-sm">
        <div className="flex items-center gap-3">
          <span className="text-lg font-bold tracking-tight">BizAgent</span>
          {datasetName && (
            <span className="hidden text-sm text-muted-foreground sm:inline">
              — {datasetName}
            </span>
          )}
        </div>
        <StatusPill state={uiState} />
      </header>

      {/* Main layout */}
      <div className="flex flex-1 gap-0">
        {/* Sidebar */}
        <aside className="w-72 shrink-0 space-y-4 border-r border-border bg-muted/20 p-4">
          <DatasetUploader
            onUploaded={handleUploaded}
            onError={handleError}
          />
          <KpiRunPanel
            datasetId={datasetId}
            onKpisReady={handleKpisReady}
            onError={handleError}
          />
        </aside>

        {/* Content area */}
        <main className="flex-1 overflow-auto p-6">
          {error && (
            <ErrorBanner
              message={error}
              onDismiss={() => setError(null)}
              className="mb-4"
            />
          )}

          {!hasData && (
            <div className="flex h-full min-h-64 items-center justify-center text-sm text-muted-foreground">
              <div className="text-center space-y-1">
                <p className="text-base font-medium text-foreground">
                  No data yet
                </p>
                <p>Upload a CSV and run KPIs to see analytics.</p>
              </div>
            </div>
          )}

          {hasData && (
            <div className="space-y-6">
              <HeadlineCards mrrRows={mrrRows} churnRows={churnRows} />

              <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                <MrrChart rows={mrrRows} />
                <RetentionChart rows={churnRows} />
              </div>

              <InsightsPanel
                datasetId={datasetId}
                mrrRows={mrrRows}
                onError={handleError}
                onThinking={handleThinking}
              />

              <AskPanel
                datasetId={datasetId}
                month={selectedMonth}
                onError={handleError}
                onThinking={handleThinking}
              />
            </div>
          )}
        </main>
      </div>
    </div>
  );
}
