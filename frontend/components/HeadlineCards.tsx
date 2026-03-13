"use client";

import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { formatCurrency, formatPercent } from "@/lib/date";
import type { MrrRow, ChurnRow } from "@/lib/types";

interface HeadlineCardsProps {
  mrrRows: MrrRow[];
  churnRows: ChurnRow[];
}

function DeltaBadge({ pct }: { pct: number | null }) {
  if (pct == null) return <span className="text-xs text-muted-foreground">—</span>;

  const positive = pct >= 0;
  const Icon = pct === 0 ? Minus : positive ? TrendingUp : TrendingDown;
  const colour = pct === 0
    ? "text-muted-foreground"
    : positive
    ? "text-emerald-600 dark:text-emerald-400"
    : "text-rose-600 dark:text-rose-400";

  return (
    <span className={`inline-flex items-center gap-0.5 text-xs font-medium ${colour}`}>
      <Icon className="h-3 w-3" />
      {Math.abs(pct).toFixed(1)}%
    </span>
  );
}

export function HeadlineCards({ mrrRows, churnRows }: HeadlineCardsProps) {
  if (!mrrRows.length) return null;

  const latest = mrrRows[mrrRows.length - 1];
  const prev = mrrRows.length > 1 ? mrrRows[mrrRows.length - 2] : null;
  const latestChurn = churnRows.length ? churnRows[churnRows.length - 1] : null;

  const mrrDelta =
    latest.mrr != null && prev?.mrr
      ? ((latest.mrr - prev.mrr) / prev.mrr) * 100
      : null;
  const netNewDelta =
    latest.net_new_mrr != null && prev?.net_new_mrr != null && prev.net_new_mrr !== 0
      ? ((latest.net_new_mrr - prev.net_new_mrr) / Math.abs(prev.net_new_mrr)) * 100
      : null;
  const churnDelta =
    latestChurn?.customer_churn_rate != null &&
    churnRows.length > 1 &&
    churnRows[churnRows.length - 2].customer_churn_rate != null
      ? (latestChurn.customer_churn_rate -
          churnRows[churnRows.length - 2].customer_churn_rate!) *
        100
      : null;

  const cards = [
    {
      label: "MRR",
      value: formatCurrency(latest.mrr),
      delta: <DeltaBadge pct={mrrDelta} />,
      sub: "Monthly Recurring Revenue",
    },
    {
      label: "Net New MRR",
      value: formatCurrency(latest.net_new_mrr),
      delta: <DeltaBadge pct={netNewDelta} />,
      sub: "New + Expansion − Churn − Contraction",
    },
    {
      label: "Customer Churn",
      value: formatPercent(latestChurn?.customer_churn_rate),
      delta:
        churnDelta != null ? (
          <span
            className={`inline-flex items-center gap-0.5 text-xs font-medium ${
              churnDelta <= 0
                ? "text-emerald-600 dark:text-emerald-400"
                : "text-rose-600 dark:text-rose-400"
            }`}
          >
            {churnDelta > 0 ? "+" : ""}
            {churnDelta.toFixed(2)} pp
          </span>
        ) : (
          <span className="text-xs text-muted-foreground">—</span>
        ),
      sub: "Churned customers / prior active",
    },
    {
      label: "NRR",
      value: formatPercent(latestChurn?.nrr),
      delta: null,
      sub: "Net Revenue Retention",
    },
  ];

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {cards.map((c) => (
        <Card key={c.label}>
          <CardContent className="p-4">
            <p className="text-xs font-medium text-muted-foreground">{c.label}</p>
            <p className="mt-1 text-2xl font-bold leading-none tracking-tight">
              {c.value}
            </p>
            <div className="mt-1.5 flex items-center gap-1.5">
              {c.delta}
            </div>
            <p className="mt-2 text-[10px] text-muted-foreground">{c.sub}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
