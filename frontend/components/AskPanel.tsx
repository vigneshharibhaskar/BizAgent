"use client";

import { useState, useRef } from "react";
import {
  MessageSquare,
  ArrowRight,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Activity,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { askAgent } from "@/lib/api";
import { AgentProgress } from "@/components/AgentProgress";
import type { AskResponse, AgentTrace } from "@/lib/types";

interface AskPanelProps {
  datasetId: string | null;
  month?: string;
  onError: (message: string) => void;
  onThinking: (v: boolean) => void;
}

export function AskPanel({
  datasetId,
  month,
  onError,
  onThinking,
}: AskPanelProps) {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [resolvedTools, setResolvedTools] = useState<string[] | null>(null);
  const [response, setResponse] = useState<AskResponse | null>(null);
  const [debugToggle, setDebugToggle] = useState(false);
  const [traceOpen, setTraceOpen] = useState(false);
  const resultsRef = useRef<HTMLDivElement>(null);
  // Monotonically increasing counter — each call to handleAsk claims a new ID.
  // Async continuations compare their captured ID against the current value to
  // detect whether a newer request has superseded them.
  const requestIdRef = useRef(0);

  async function handleAsk() {
    if (!datasetId || !query.trim()) return;
    const requestId = ++requestIdRef.current;

    setLoading(true);
    setResolvedTools(null);
    onThinking(true);
    try {
      const res = await askAgent(datasetId, query.trim(), month, debugToggle);

      if (requestId !== requestIdRef.current) return; // superseded — discard
      setResponse(res);

      // Flash real tool names as "all done" for 600 ms before showing results.
      const realTools = res.trace?.tool_calls ?? [];
      if (realTools.length > 0) {
        setResolvedTools(realTools);
        await new Promise<void>((r) => setTimeout(r, 600));
        if (requestId !== requestIdRef.current) return; // superseded during pause
      }
    } catch (err) {
      if (requestId !== requestIdRef.current) return; // stale error — ignore
      onError(err instanceof Error ? err.message : "Ask request failed");
    } finally {
      // Only the winning request cleans up shared UI state.
      if (requestId === requestIdRef.current) {
        setLoading(false);
        onThinking(false);
        setTimeout(
          () => resultsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }),
          50
        );
      }
    }
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) {
      handleAsk();
    }
  }

  const analysis = response?.analysis;
  const trace = response?.trace;

  function typeBadgeVariant(type: string) {
    if (type === "forecast") return "secondary" as const;
    if (type === "explanation") return "warning" as const;
    return "info" as const;
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="flex items-center gap-2 text-base">
            <MessageSquare className="h-4 w-4" />
            Ask BizAgent
          </CardTitle>
          <label className="flex cursor-pointer select-none items-center gap-1.5 text-xs text-muted-foreground">
            <input
              type="checkbox"
              checked={debugToggle}
              onChange={(e) => setDebugToggle(e.target.checked)}
              className="rounded"
            />
            debug
          </label>
        </div>
      </CardHeader>

      <CardContent className="space-y-4">
        {/* Input area */}
        <div className="space-y-2">
          <textarea
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={
              datasetId
                ? "Ask anything about your data… (⌘+Enter to send)"
                : "Upload a dataset first"
            }
            disabled={!datasetId || loading}
            rows={3}
            className="w-full resize-none rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
          />
          <Button
            onClick={handleAsk}
            disabled={!datasetId || !query.trim() || loading}
            size="sm"
            className="w-full"
          >
            {loading ? "Thinking…" : "Ask"}
          </Button>
        </div>

        {/* Agent progress */}
        {loading && <AgentProgress completedSteps={resolvedTools ?? undefined} />}

        {/* Results */}
        {analysis && !loading && (
          <div ref={resultsRef} className="space-y-4">
            {/* Header row */}
            <div className="flex flex-wrap items-start justify-between gap-2">
              <p className="font-semibold leading-snug">{analysis.title}</p>
              <div className="flex items-center gap-1.5">
                <Badge variant={typeBadgeVariant(analysis.type)}>
                  {analysis.type.replace("_", " ")}
                </Badge>
                {response?.month && (
                  <Badge variant="outline">{response.month}</Badge>
                )}
              </div>
            </div>

            {/* Confidence bar */}
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
                      <p className="text-sm font-medium">{a.title}</p>
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

            {/* Agent trace (debug mode) */}
            {debugToggle && trace && (
              <AgentTracePanel
                trace={trace}
                open={traceOpen}
                onToggle={() => setTraceOpen((v) => !v)}
              />
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

// ---------------------------------------------------------------------------
// Agent trace sub-component
// ---------------------------------------------------------------------------

function AgentTracePanel({
  trace,
  open,
  onToggle,
}: {
  trace: AgentTrace;
  open: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="rounded-md border border-border">
      <button
        className="flex w-full items-center justify-between px-3 py-2 text-xs font-medium text-muted-foreground"
        onClick={onToggle}
      >
        <span className="flex items-center gap-1.5">
          <Activity className="h-3.5 w-3.5" />
          Agent Trace
        </span>
        {open ? (
          <ChevronUp className="h-3.5 w-3.5" />
        ) : (
          <ChevronDown className="h-3.5 w-3.5" />
        )}
      </button>

      {open && (
        <div className="divide-y divide-border">
          <TraceRow label="Model" value={trace.model || "—"} />
          <TraceRow
            label="Planner steps"
            value={trace.agent_plan.join(" → ") || "—"}
          />
          <TraceRow
            label="Tools called"
            value={trace.tool_calls.join(" → ") || "—"}
          />
          <TraceRow label="Iterations" value={String(trace.iterations)} />
          <TraceRow
            label="Scenario run"
            value={trace.scenario_run ? "yes" : "no"}
          />
        </div>
      )}
    </div>
  );
}

function TraceRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-start justify-between gap-4 px-3 py-1.5 text-xs">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span className="break-all font-mono text-right">{value}</span>
    </div>
  );
}
