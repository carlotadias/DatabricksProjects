import { describe, expect, it } from "vitest";
import { dataApiSnippets, dailyMpanPath, postgresDatabase } from "./snippets";

describe("Data API snippets", () => {
  const base = "https://lakebase.example.com/api/2.0/workspace/1/rest/databricks_postgres";
  it("uses the same PostgREST path shape as the app and no token", () => {
    expect(dailyMpanPath("1000000271828", "2026-10-02", "2026-10-08")).toBe(
      "/portal_gold/daily_mpan?select=reading_date,kwh,total_gbp&mpan=eq.1000000271828" +
      "&reading_date=gte.2026-10-02&reading_date=lte.2026-10-08&order=reading_date.asc");
    const s = dataApiSnippets(base, "1000000271828", "2026-10-02", "2026-10-08");
    expect(s.http.split("\n")[0]).toBe(`GET ${base}${s.path}`);
    expect(s.http).toContain("Authorization: Bearer <OAuth token>");
    expect(s.python).toContain('("mpan", "eq.1000000271828")');
    expect(s.curl).toContain(`"${base}${s.path}"`);
    for (const t of [s.http, s.python, s.curl]) expect(t).not.toMatch(/eyJ|dapi[0-9a-f]{8}/);
  });
  it("reads the Postgres database from the Data API URL", () => {
    expect(postgresDatabase(base)).toBe("databricks_postgres");
    expect(postgresDatabase("https://x.example/rest")).toBe("databricks_postgres");
    expect(postgresDatabase("https://x.example/api/rest/other_db")).toBe("other_db");
  });
  it("falls back to a placeholder base URL", () => {
    expect(dataApiSnippets(null, "1", "2026-01-01", "2026-01-02").http).toMatch(/^GET <DATA_API_URL>\/portal_gold/);
  });
});
