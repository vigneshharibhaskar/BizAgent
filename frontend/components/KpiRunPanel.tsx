"use client";

import { useState } from "react";
import { Play, ChevronDown, ChevronUp, AlertTriangle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { runKpis } from "@/lib/api";
import type { KpiRunResponse } from "@/lib/types";

interface KpiRunPanelProps {
  datasetId: string | null;
  onKpisReady: () => void;
  onError: (message: string) => void;
}

export function KpiRunPanel({ datasetId, onKpisReady, onError }: KpiRunPanelProps) {
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<KpiRunResponse | null>(null);
  const [warningsOpen, setWarningsOpen] = useState(false);

  async function handleRun() {
    if (!datasetId) return;
    setLoading(true);
    try {
      const res = await runKpis(datasetId);
      setResult(res);
      onKpisReady();
    } catch (err) {
      onError(err instanceof Error ? err.message : "KPI run failed");
    } finally {
      setLoading(false);
    }
  }

  const warnings = result?.warnings ?? [];

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Play className="h-4 w-4" />
          KPI Engine
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <Button
          onClick={handleRun}
          disabled={!datasetId || loading}
          className="w-full"
          size="sm"
        >
          {loading ? "Running…" : "Run KPIs"}
        </Button>

        {result && (
          <div className="space-y-2 text-xs">
            <div className="grid grid-cols-3 gap-2 text-center">
              <div className="rounded-md bg-muted p-2">
                <p className="text-lg font-bold leading-none text-foreground">
                  {result.months_computed}
                </p>
                <p className="mt-1 text-muted-foreground">months</p>
              </div>
              <div className="rounded-md bg-muted p-2">
                <p className="text-lg font-bold leading-none text-foreground">
                  {result.segments_computed}
                </p>
                <p className="mt-1 text-muted-foreground">segments</p>
              </div>
              <div className="rounded-md bg-muted p-2">
                <p className="text-lg font-bold leading-none text-foreground">
                  {result.cohorts_computed}
                </p>
                <p className="mt-1 text-muted-foreground">cohorts</p>
              </div>
            </div>

            {warnings.length > 0 && (
              <div className="rounded-md border border-amber-200 bg-amber-50 dark:border-amber-900 dark:bg-amber-950">
                <button
                  className="flex w-full items-center justify-between px-3 py-2 text-xs font-medium text-amber-700 dark:text-amber-300"
                  onClick={() => setWarningsOpen((v) => !v)}
                >
                  <span className="flex items-center gap-1.5">
                    <AlertTriangle className="h-3.5 w-3.5" />
                    {warnings.length} warning{warnings.length !== 1 ? "s" : ""}
                  </span>
                  {warningsOpen ? (
                    <ChevronUp className="h-3.5 w-3.5" />
                  ) : (
                    <ChevronDown className="h-3.5 w-3.5" />
                  )}
                </button>
                {warningsOpen && (
                  <ul className="divide-y divide-amber-100 dark:divide-amber-900">
                    {warnings.map((w, i) => (
                      <li
                        key={i}
                        className="px-3 py-1.5 text-xs text-amber-600 dark:text-amber-400"
                      >
                        {w}
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
            Upload a dataset first
          </p>
        )}
      </CardContent>
    </Card>
  );
}
