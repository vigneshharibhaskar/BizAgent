"use client";

import { Activity, Loader2, Mic, Volume2 } from "lucide-react";
import { cn } from "@/lib/utils";
import type { UIState } from "@/lib/types";

interface StatusPillProps {
  state: UIState;
  className?: string;
}

const STATE_CONFIG: Record<
  UIState,
  { label: string; icon: React.ReactNode; className: string }
> = {
  idle: {
    label: "Idle",
    icon: <Activity className="h-3.5 w-3.5" />,
    className: "bg-slate-100 text-slate-600 border-slate-200",
  },
  thinking: {
    label: "Thinking…",
    icon: <Loader2 className="h-3.5 w-3.5 animate-spin" />,
    className: "bg-violet-100 text-violet-700 border-violet-200",
  },
  listening: {
    label: "Listening",
    icon: <Mic className="h-3.5 w-3.5" />,
    className: "bg-blue-100 text-blue-700 border-blue-200",
  },
  transcribing: {
    label: "Transcribing…",
    icon: <Loader2 className="h-3.5 w-3.5 animate-spin" />,
    className: "bg-amber-100 text-amber-700 border-amber-200",
  },
  speaking: {
    label: "Speaking",
    icon: <Volume2 className="h-3.5 w-3.5" />,
    className: "bg-emerald-100 text-emerald-700 border-emerald-200",
  },
};

export function StatusPill({ state, className }: StatusPillProps) {
  const config = STATE_CONFIG[state];

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium",
        config.className,
        className
      )}
    >
      {config.icon}
      {config.label}
    </span>
  );
}
