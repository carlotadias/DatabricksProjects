import { describe, expect, it } from "vitest";
import { addMonths } from "./dates";
import {
  canShift, clampRange, comparisonNoun, covered, granularityFor, padRows, periodLabel, periodRange, previousRange,
  shiftAnchor,
} from "./periods";

const B = { first: "2025-09-06", last: "2026-10-05" };

describe("period ranges", () => {
  it("builds calendar periods; weeks start on Monday", () => {
    expect(periodRange("day", "2026-10-05")).toEqual({ start: "2026-10-05", end: "2026-10-05" });
    expect(periodRange("week", "2026-10-05")).toEqual({ start: "2026-10-05", end: "2026-10-11" }); // a Monday
    expect(periodRange("week", "2026-10-04")).toEqual({ start: "2026-09-28", end: "2026-10-04" }); // a Sunday
    expect(periodRange("month", "2026-02-14")).toEqual({ start: "2026-02-01", end: "2026-02-28" });
    expect(periodRange("month", "2024-02-14")).toEqual({ start: "2024-02-01", end: "2024-02-29" });
    expect(periodRange("year", "2026-10-05")).toEqual({ start: "2026-01-01", end: "2026-12-31" });
  });

  it("clamps to the data window", () => {
    expect(clampRange(periodRange("month", "2026-10-05"), B)).toEqual({ start: "2026-10-01", end: "2026-10-05" });
    expect(clampRange(periodRange("year", "2025-01-01"), B)).toEqual({ start: "2025-09-06", end: "2025-12-31" });
    expect(clampRange({ start: "2024-01-01", end: "2024-01-31" }, B)).toBeNull();
  });

  it("moves with prev/next and stops at the data window", () => {
    expect(shiftAnchor("day", "2026-10-05", -1)).toBe("2026-10-04");
    expect(shiftAnchor("week", "2026-10-05", 1)).toBe("2026-10-12");
    expect(shiftAnchor("month", "2026-03-31", -1)).toBe("2026-02-28");
    expect(shiftAnchor("year", "2026-10-05", -1)).toBe("2025-10-05");
    expect(canShift("day", "2026-10-05", 1, B)).toBe(false);
    expect(canShift("day", "2026-10-05", -1, B)).toBe(true);
    expect(canShift("month", "2025-09-10", -1, B)).toBe(false);
    expect(canShift("year", "2026-10-05", -1, B)).toBe(true);
  });
});

describe("comparison period", () => {
  it("is the same span one period earlier", () => {
    expect(previousRange("day", { start: "2026-10-05", end: "2026-10-05" })).toEqual(
      { start: "2026-10-04", end: "2026-10-04" });
    expect(previousRange("week", { start: "2026-09-28", end: "2026-10-04" })).toEqual(
      { start: "2026-09-21", end: "2026-09-27" });
    // month to date compares with the same days of the previous month
    expect(previousRange("month", { start: "2026-10-01", end: "2026-10-05" })).toEqual(
      { start: "2026-09-01", end: "2026-09-05" });
    expect(previousRange("month", { start: "2026-03-01", end: "2026-03-31" })).toEqual(
      { start: "2026-02-01", end: "2026-02-28" });
    expect(previousRange("year", { start: "2026-01-01", end: "2026-10-05" })).toEqual(
      { start: "2025-01-01", end: "2025-10-05" });
    expect(previousRange("custom", { start: "2026-09-10", end: "2026-09-19" })).toEqual(
      { start: "2026-08-31", end: "2026-09-09" });
  });

  it("knows when the previous period is outside the 13-month history", () => {
    expect(covered({ start: "2025-01-01", end: "2025-10-05" }, B)).toBe(false);
    expect(covered({ start: "2026-09-01", end: "2026-09-05" }, B)).toBe(true);
    expect(comparisonNoun("week")).toBe("previous week");
    expect(comparisonNoun("custom")).toBe("previous period");
  });
});

describe("granularity and labels", () => {
  it("picks bars per tab", () => {
    expect(granularityFor("day", { start: "2026-10-05", end: "2026-10-05" })).toBe("halfhour");
    expect(granularityFor("week", { start: "2026-09-28", end: "2026-10-04" })).toBe("day");
    expect(granularityFor("year", { start: "2026-01-01", end: "2026-10-05" })).toBe("month");
    expect(granularityFor("custom", { start: "2026-07-01", end: "2026-09-30" })).toBe("day");
    expect(granularityFor("custom", { start: "2026-01-01", end: "2026-09-30" })).toBe("month");
  });

  it("labels the period", () => {
    expect(periodLabel("day", { start: "2025-03-01", end: "2025-03-01" })).toBe("1 March 2025");
    expect(periodLabel("week", { start: "2026-09-28", end: "2026-10-04" })).toBe("28 September 2026 - 4 October 2026");
    expect(periodLabel("month", { start: "2026-10-01", end: "2026-10-05" })).toBe("October 2026");
    expect(periodLabel("year", { start: "2026-01-01", end: "2026-10-05" })).toBe("2026");
  });

  it("adds months like the backend (clamped)", () => {
    expect(addMonths("2026-01-31", 1)).toBe("2026-02-28");
    expect(addMonths("2026-01-15", -13)).toBe("2024-12-15");
  });
});

describe("padRows", () => {
  const row = (t: string) => ({ t, kwh: 1, consumption_gbp: 1, standing_gbp: 0.5, total_gbp: 1.5 });
  it("fills the rest of a partial week or year with empty slots", () => {
    const out = padRows([row("2026-10-05")], { start: "2026-10-05", end: "2026-10-11" }, "day");
    expect(out.map((r) => [r.t, !!r.missing])).toEqual([["2026-10-05", false], ["2026-10-06", true], ["2026-10-07", true],
      ["2026-10-08", true], ["2026-10-09", true], ["2026-10-10", true], ["2026-10-11", true]]);
    const year = padRows([row("2026-01-01"), row("2026-10-01")], { start: "2026-01-01", end: "2026-12-31" }, "month");
    expect(year).toHaveLength(12);
    expect(year.filter((r) => !r.missing).map((r) => r.t)).toEqual(["2026-01-01", "2026-10-01"]);
  });
  it("leaves half-hourly rows alone", () => {
    const rows = [row("2026-10-05T00:00:00+00:00")];
    expect(padRows(rows, { start: "2026-10-05", end: "2026-10-05" }, "halfhour")).toBe(rows);
  });
});
