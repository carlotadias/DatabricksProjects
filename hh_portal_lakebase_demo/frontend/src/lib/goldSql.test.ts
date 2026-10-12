import { describe, expect, it } from "vitest";
import gold from "../../../sql/lakebase_gold.sql?raw";
import { fullSql, highlightSql, parseGold, splitStatements } from "./goldSql";

describe("gold SQL from the versioned file", () => {
  const objs = parseGold(gold);

  it("lists every view and materialized view with its indexes", () => {
    const names = objs.map((o) => `${o.schema}.${o.name}`);
    expect(names).toContain("portal_gold.daily_mpan");
    expect(names).toContain("portal_work.hh_account_totals");
    expect(names).toContain("portal_gold.hh_mpan");
    expect(objs.filter((o) => o.kind === "MATERIALIZED VIEW")).toHaveLength(10);
    expect(objs.filter((o) => o.kind === "VIEW")).toHaveLength(6);
    for (const o of objs.filter((x) => x.kind === "MATERIALIZED VIEW")) {
      expect(o.indexes.some((i) => i.startsWith("CREATE UNIQUE INDEX"))).toBe(true);
    }
  });

  it("derives the nightly refresh like the job does", () => {
    const daily = objs.find((o) => o.name === "daily_mpan")!;
    expect(daily.refresh).toBe("REFRESH MATERIALIZED VIEW CONCURRENTLY portal_gold.daily_mpan;");
    const work = objs.find((o) => o.name === "hh_account_totals")!;
    expect(work.refresh).toBe("REFRESH MATERIALIZED VIEW portal_work.hh_account_totals;");
    expect(work.exposed).toBe(false);
    expect(objs.find((o) => o.name === "hh_mpan")!.refresh).toBeNull();
    expect(fullSql(daily)).toMatch(/^CREATE MATERIALIZED VIEW IF NOT EXISTS portal_gold\.daily_mpan AS[\s\S]*daily_mpan_pk[\s\S]*REFRESH/);
  });

  it("splits on line-final semicolons and drops comments", () => {
    expect(splitStatements("-- c; x\nSELECT 1;\nSELECT 2;\n")).toEqual(["SELECT 1", "SELECT 2"]);
  });

  it("highlights keywords and comments only", () => {
    expect(highlightSql("SELECT kwh -- note\nFROM t")).toEqual([
      { t: "SELECT", k: "kw" }, { t: " " }, { t: "kwh" }, { t: " " }, { t: "-- note", k: "comment" }, { t: "\n" },
      { t: "FROM", k: "kw" }, { t: " " }, { t: "t" }]);
  });
});
