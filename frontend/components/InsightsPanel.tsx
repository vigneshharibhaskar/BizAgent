"use client";

import { useState } from "react";
import {
  Sparkles,
  ChevronDown,
  ChevronUp,
  CheckCircle2,
  ArrowRight,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { generateInsights } from "@/lib/api";
import { latestMonth, toYearMonth, formatMonth } from "@/lib/date";
import type { InsightGenerateResponse, MrrRow } from "@/lib/types";

interface InsightsPanelProps {
  datasetId: string | null;
  mrrRows: MrrRow[];
  onError: (message: string) => void;
  onThinking: (v: boolean) => void;
}

export function InsightsPanel({
  datasetId,
  mrrRows,
  onError,
  onThinking,
}: InsightsPanelProps) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<InsightGenerateResponse | null>(null);
  const [assumptionsOpen, setAssumptionsOpen] = useState(false);
  const [selectedMonth, setSelectedMonth] = useState<string>("");

  // Derive available month options from mrrRows
  const monthOptions = mrrRows.map((r) => toYearMonth(r.month));
  const defaultMonth = latestMonth(mrrRows.map((r) => r.month)) ?? "";
  const month = selectedMonth || defaultMonth;

  async function handleGenerate() {
    if (!datasetId || !month) return;
    setLoading(true);
    onThinking(true);
    try {
      const res = await generateInsights(datasetId, month);
      setResult(res);
    } catch (err) {
      onError(err instanceof Error ? err.message : "Insight generation failed");
    } finally {
      setLoading(false);
      onThinking(false);
    }
  }

  const analysis = result?.analysis;

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Sparkles className="h-4 w-4" />
          AI Insights
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Controls */}
        <div className="flex items-center gap-2">
          <select
            value={month}
            onChange={(e) => setSelectedMonth(e.target.value)}
            disabled={!monthOptions.length}
            className="flex-1 rounded-md border border-input bg-background px-3 py-1.5 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
          >
            {monthOptions.length === 0 ? (
              <option value="">— run KPIs first —</option>
            ) : (
              monthOptions.map((m) => (
                <option key={m} value={m}>
                  {formatMonth(m)}
                </option>
              ))
            )}
          </select>
          <Button
            onClick={handleGenerate}
            disabled={!datasetId || !month || loading}
            size="sm"
          >
            {loading ? "Generating…" : "Generate"}
          </Button>
        </div>

        {/* Loading skeleton */}
        {loading && (
          <div className="space-y-2 animate-pulse">
            <div className="h-4 w-3/4 rounded bg-muted" />
            <div className="h-4 w-1/2 rounded bg-muted" />
            <div className="h-4 w-2/3 rounded bg-muted" />
          </div>
        )}

        {/* Results */}
        {analysis && !loading && (
          <div className="space-y-4">
            {/* Header row */}
            <div className="flex flex-wrap items-start justify-between gap-2">
              <p className="font-semibold leading-snug">{analysis.title}</p>
              <Badge variant={result?.cached ? "secondary" : "success"}>
                {result?.cached ? "cached" : "fresh"}
              </Badge>
            </div>

            {/* Summary bullets */}
            <ul className="space-y-1.5">
              {analysis.summary_bullets.map((b, i) => (
                <li key={i} className="flex items-start gap-2 text-sm">
                  <span className="mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded-full bg-primary/10 text-[10px] font-bold text-primary">
                    {i + 1}
                  </span>
                  {b}
                </li>
              ))}
            </ul>

            {/* Confidence */}
            <div>
              <div className="mb-1 flex items-center justify-between text-xs text-muted-foreground">
                <span>Confidence</span>
                <span>{(analysis.confidence * 100).toFixed(0)}%</span>
              </div>
              <div className="h-1.5 w-full rounded-full bg-muted">
                <div
                  className="h-1.5 rounded-full bg-primary transition-all"
                  style={{ width: `${analysis.confidence * 100}%` }}
                />
              </div>
            </div>

            {/* Prioritized actions */}
            <div>
              <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Priority Actions
              </p>
              <div className="space-y-2">
                {analysis.prioritized_actions.map((a) => (
                  <div
                    key={a.priority}
                    className="rounded-lg border border-border bg-muted/40 p-3"
                  >
                    <div className="flex items-center gap-2">
                      <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-primary text-[10px] font-bold text-primary-foreground">
                        {a.priority}
                      </span>
                      <p className="font-medium text-sm">{a.title}</p>
                    </div>
                    <p className="mt-1 text-xs text-muted-foreground">
                      {a.rationale}
                    </p>
                    <p className="mt-1 flex items-center gap-1 text-xs text-emerald-600 dark:text-emerald-400">
                      <ArrowRight className="h-3 w-3" />
                      {a.expected_impact}
                    </p>
                  </div>
                ))}
              </div>
            </div>

            {/* Key numbers */}
            {Object.keys(analysis.key_numbers).length > 0 && (
              <div>
                <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  Key Numbers
                </p>
                <div className="grid grid-cols-2 gap-1.5">
                  {Object.entries(analysis.key_numbers).map(([k, v]) => (
                    <div
                      key={k}
                      className="flex items-center justify-between rounded-md bg-muted px-2 py-1.5 text-xs"
                    >
                      <span className="text-muted-foreground">{k}</span>
                      <span className="font-mono font-medium">
                        {v != null ? v.toLocaleString() : "—"}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Next checks */}
            <div>
              <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Next Checks
              </p>
              <ul className="space-y-1">
                {analysis.next_checks.map((c, i) => (
                  <li key={i} className="flex items-start gap-2 text-xs">
                    <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                    {c}
                  </li>
                ))}
              </ul>
            </div>

            {/* Assumptions (collapsible) */}
            {analysis.assumptions.length > 0 && (
              <div className="rounded-md border border-border">
                <button
                  className="flex w-full items-center justify-between px-3 py-2 text-xs font-medium text-muted-foreground"
                  onClick={() => setAssumptionsOpen((v) => !v)}
                >
                  <span>
                    Assumptions & caveats ({analysis.assumptions.length})
                  </span>
                  {assumptionsOpen ? (
                    <ChevronUp className="h-3.5 w-3.5" />
                  ) : (
                    <ChevronDown className="h-3.5 w-3.5" />
                  )}
                </button>
                {assumptionsOpen && (
                  <ul className="divide-y divide-border">
                    {analysis.assumptions.map((a, i) => (
                      <li
                        key={i}
                        className="px-3 py-1.5 text-xs text-muted-foreground"
                      >
                        {a}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
          </div>
        )}

        {!datasetId && (
          <p className="text-center text-xs text-muted-foreground">
            Upload a dataset and run KPIs first
          </p>
        )}
      </CardContent>
    </Card>
  );
}
