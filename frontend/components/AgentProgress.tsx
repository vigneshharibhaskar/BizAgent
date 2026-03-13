"use client";

import { useEffect, useRef, useState } from "react";
import { CheckCircle2, Circle } from "lucide-react";

const DEFAULT_STEPS = [
  "Analyzing KPI trends",
  "Checking churn segments",
  "Reviewing cohort retention",
  "Generating recommendations",
];

const STEP_INTERVAL_MS = 700;

interface AgentProgressProps {
  /**
   * When provided, immediately show these steps as all-done (no animation).
   * Used to sync with real trace.tool_calls once the response arrives.
   */
  completedSteps?: string[];
}

export function AgentProgress({ completedSteps }: AgentProgressProps) {
  const allDone = completedSteps != null;
  const displaySteps = completedSteps ?? DEFAULT_STEPS;

  // How many DEFAULT_STEPS have been "animated past" (only meaningful while !allDone)
  const [animatedIndex, setAnimatedIndex] = useState(0);

  // Stable ref to the current pending timer — lets us clear on unmount or prop change
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Clear any pending timer on unmount
  useEffect(() => {
    return () => {
      if (timerRef.current != null) clearTimeout(timerRef.current);
    };
  }, []);

  // Advance one step every STEP_INTERVAL_MS while animating
  useEffect(() => {
    if (allDone) {
      // Real steps arrived — stop any in-flight timer immediately
      if (timerRef.current != null) clearTimeout(timerRef.current);
      return;
    }
    if (animatedIndex >= DEFAULT_STEPS.length - 1) return;

    timerRef.current = setTimeout(
      () => setAnimatedIndex((i) => i + 1),
      STEP_INTERVAL_MS
    );

    return () => {
      if (timerRef.current != null) clearTimeout(timerRef.current);
    };
  }, [animatedIndex, allDone]);

  // doneCount: how many items in displaySteps should show a ✓
  const doneCount = allDone ? displaySteps.length : animatedIndex;

  return (
    <div className="space-y-3 py-1">
      <p className="text-sm font-medium text-foreground">
        {allDone ? "BizAgent completed analysis" : "BizAgent is analyzing…"}
      </p>
      <ul className="space-y-2">
        {displaySteps.map((step, i) => {
          const done = i < doneCount;
          const active = i === doneCount && !allDone;

          return (
            <li key={`${step}-${i}`} className="flex items-center gap-2.5 text-sm">
              {done ? (
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-500 dark:text-emerald-400" />
              ) : active ? (
                <Circle className="h-4 w-4 shrink-0 animate-pulse text-primary" />
              ) : (
                <Circle className="h-4 w-4 shrink-0 text-muted-foreground/40" />
              )}

              <span
                className={
                  done && allDone
                    ? "text-foreground" // real completed steps — no strikethrough
                    : done
                    ? "text-muted-foreground line-through" // animated past step
                    : active
                    ? "font-medium text-foreground"
                    : "text-muted-foreground/50"
                }
              >
                {step}
                {active && (
                  <span className="ml-0.5 inline-flex gap-px">
                    <span className="animate-bounce [animation-delay:0ms]">.</span>
                    <span className="animate-bounce [animation-delay:150ms]">.</span>
                    <span className="animate-bounce [animation-delay:300ms]">.</span>
                  </span>
                )}
              </span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
