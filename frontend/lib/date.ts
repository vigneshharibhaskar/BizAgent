/**
 * Convert a "YYYY-MM-DD" date string to "YYYY-MM" period string.
 * Backend stores months as first-of-month dates; charts need just year+month.
 */
export function toYearMonth(dateStr: string): string {
  return dateStr.slice(0, 7);
}

/**
 * Format a "YYYY-MM" or "YYYY-MM-DD" string to a human-readable month label.
 * e.g. "2024-03" → "Mar 2024"
 */
export function formatMonth(value: string): string {
  const ym = value.slice(0, 7);
  const [year, month] = ym.split("-");
  const d = new Date(Number(year), Number(month) - 1, 1);
  return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
}

/**
 * Format a number as currency (USD, no cents for large values).
 * e.g. 125000 → "$125,000"
 */
export function formatCurrency(value: number | null | undefined): string {
  if (value == null) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

/**
 * Format a ratio (0–1) as a percentage string.
 * e.g. 0.0523 → "5.2%"
 */
export function formatPercent(
  value: number | null | undefined,
  decimals = 1
): string {
  if (value == null) return "—";
  return `${(value * 100).toFixed(decimals)}%`;
}

/**
 * Format an NRR/GRR ratio as a multiple string.
 * e.g. 1.12 → "1.12×"
 */
export function formatMultiple(value: number | null | undefined): string {
  if (value == null) return "—";
  return `${value.toFixed(2)}×`;
}

/**
 * Return the most recent "YYYY-MM" from an array of "YYYY-MM-DD" month strings.
 */
export function latestMonth(months: string[]): string | null {
  if (!months.length) return null;
  const sorted = [...months].sort();
  return toYearMonth(sorted[sorted.length - 1]);
}
