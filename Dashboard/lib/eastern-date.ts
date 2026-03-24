import { TZDate } from "@date-fns/tz";
import { startOfDay, endOfDay } from "date-fns";

export const EASTERN_TZ = "America/New_York";

/** Calendar date YYYY-MM-DD in America/New_York for the given instant. */
export function easternDateYmd(d = new Date()): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: EASTERN_TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(d);
  const y = parts.find((p) => p.type === "year")!.value;
  const m = parts.find((p) => p.type === "month")!.value;
  const day = parts.find((p) => p.type === "day")!.value;
  return `${y}-${m}-${day}`;
}

/** Interpret `ymd` as an Eastern calendar day; return UTC ISO bounds for DB queries. */
export function easternYmdToUtcRange(ymd: string): { startIso: string; endIso: string } {
  const [y, mo, d] = ymd.split("-").map(Number);
  const zoned = new TZDate(y, mo - 1, d, EASTERN_TZ);
  const start = startOfDay(zoned);
  const end = endOfDay(zoned);
  return { startIso: start.toISOString(), endIso: end.toISOString() };
}
