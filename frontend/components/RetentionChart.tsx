"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatMonth, toYearMonth } from "@/lib/date";
import type { ChurnRow } from "@/lib/types";

interface RetentionChartProps {
  rows: ChurnRow[];
}

export function RetentionChart({ rows }: RetentionChartProps) {
  if (!rows.length) return null;

  const data = rows.map((r) => ({
    month: formatMonth(toYearMonth(r.month)),
    NRR: r.nrr != null ? +(r.nrr * 100).toFixed(2) : null,
    GRR: r.grr != null ? +(r.grr * 100).toFixed(2) : null,
  }));

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="text-base">Retention Rates (NRR & GRR)</CardTitle>
      </CardHeader>
      <CardContent>
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={data} margin={{ top: 4, right: 16, bottom: 0, left: 16 }}>
            <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
            <XAxis
              dataKey="month"
              tick={{ fontSize: 11 }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              tickFormatter={(v: number) => `${v}%`}
              tick={{ fontSize: 11 }}
              tickLine={false}
              axisLine={false}
              width={48}
            />
            <Tooltip
              formatter={(value: number | null, name: string) => [
                value != null ? `${value}%` : "—",
                name,
              ]}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <ReferenceLine
              y={100}
              stroke="#94a3b8"
              strokeDasharray="4 4"
              label={{ value: "100%", fontSize: 10, fill: "#94a3b8" }}
            />
            <Line
              type="monotone"
              dataKey="NRR"
              stroke="#6366f1"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4 }}
              connectNulls
            />
            <Line
              type="monotone"
              dataKey="GRR"
              stroke="#10b981"
              strokeWidth={1.5}
              dot={false}
              activeDot={{ r: 4 }}
              connectNulls
            />
          </LineChart>
        </ResponsiveContainer>
      </CardContent>
    </Card>
  );
}
