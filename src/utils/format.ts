import dayjs from "dayjs";

export function formatNumber(n: number | null | undefined): string {
  if (n == null) return "-";
  return n.toLocaleString();
}

export function formatBytes(n: number | null | undefined): string {
  if (n == null) return "-";
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 100 ? 0 : v >= 10 ? 1 : 2)} ${units[i]}`;
}

export function formatTimestamp(ms: number | null | undefined): string {
  if (ms == null) return "-";
  return dayjs(ms).format("YYYY-MM-DD HH:mm:ss.SSS");
}

export function truncate(s: string | null | undefined, n: number): string {
  if (!s) return "";
  if (s.length <= n) return s;
  return s.slice(0, n) + "…";
}

export function uuidv4(): string {
  return crypto.randomUUID();
}

export type DurationUnit = "ms" | "s" | "m" | "h" | "d";

export const DURATION_UNIT_MS: Record<DurationUnit, number> = {
  ms: 1,
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000,
  d: 24 * 60 * 60 * 1000,
};

const DURATION_UNIT_LABEL: Record<DurationUnit, string> = {
  ms: "Milliseconds",
  s: "Seconds",
  m: "Minutes",
  h: "Hours",
  d: "Days",
};

/**
 * Format a millisecond duration for display.
 * - `-1` → "Forever"
 * - exact multiple of a unit → "<n> <Unit>" (e.g. 7 Days, 168 Hours)
 * - otherwise → "~<n> <Unit>" (approx, n is floor(ms / unit))
 *
 * The chosen unit is the largest one whose value is >= 1; ties prefer the
 * larger unit so 86_400_000 reads as "1 Days" not "24 Hours".
 *
 * Returns null-style "-" for null/undefined to match other formatters.
 */
export function formatDurationMs(ms: number | null | undefined): string {
  if (ms == null) return "-";
  if (ms === -1) return "Forever";
  if (ms === 0) return "0 Milliseconds";
  if (ms < 0) return String(ms); // unexpected; show raw

  const order: DurationUnit[] = ["d", "h", "m", "s", "ms"];
  for (const u of order) {
    const factor = DURATION_UNIT_MS[u];
    if (ms >= factor) {
      const exact = ms % factor === 0;
      const n = exact ? ms / factor : Math.floor(ms / factor);
      return `${exact ? "" : "~"}${n} ${DURATION_UNIT_LABEL[u]}`;
    }
  }
  return `${ms} Milliseconds`;
}
