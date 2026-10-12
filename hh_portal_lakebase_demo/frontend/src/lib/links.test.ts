import { describe, expect, it } from "vitest";
import { buildLinks, lakebaseUrl, repoFile, type ArchConfig } from "./links";

const H = "https://workspace.example.com";
const CFG: ArchConfig = { host: H, catalog: "demo_cat", data_schema: "hh_portal_demo", synced_schema: "portal",
  gold_schema: "portal_gold", sync_pipeline_id: "pipe-1", daily_job_id: "11", setup_job_id: "22",
  daily_schedule: "0 0 2 * * ? (UTC)", lakebase_project: "hh-portal-demo", lakebase_branch: "production",
  repo_url: "https://github.com/org/repo/tree/main/hh_portal_lakebase_demo",
  lakebase_project_uid: "proj-uid-1", lakebase_branch_uid: "br-example-1",
  lakebase_data_api_tab: "data-api", data_api_url: "https://lakebase.example.com/api/2.0/workspace/1/rest/databricks_postgres" };
const LB = `${H}/lakebase/projects/proj-uid-1/branches/br-example-1`;

describe("architecture links", () => {
  it("builds workspace URLs from the bundle identifiers", () => {
    const by = Object.fromEntries(buildLinks(CFG).map((l) => [l.id, l.url]));
    expect(by.silver).toBe(`${H}/explore/data/demo_cat/hh_portal_demo/silver_hh_readings`);
    expect(by.synced).toBe(`${H}/explore/data/demo_cat/portal/silver_hh_readings`);
    expect(by.log).toBe(`${H}/explore/data/demo_cat/hh_portal_demo/portal_refresh_log`);
    expect(by.pipeline).toBe(`${H}/pipelines/pipe-1`);
    expect(by.daily).toBe(`${H}/jobs/11`);
    expect(by.setup).toBe(`${H}/jobs/22`);
    expect(by.project).toBe(`${LB}/tables`);
    expect(by["data-api"]).toBe(`${LB}/data-api`);
    expect(by.projects).toBeUndefined();
    expect(Object.values(by).some((u) => u === CFG.data_api_url)).toBe(false); // the REST base is never a link
    expect(by.code).toBe(CFG.repo_url);
    expect(by.runbook).toBe("https://github.com/org/repo/blob/main/hh_portal_lakebase_demo/docs/RUNBOOK.md");
    expect(by.adr).toBe("https://github.com/org/repo/blob/main/hh_portal_lakebase_demo/docs/ADR.md");
  });

  it("labels the single Lakebase tile and splits it when a Data API tab path is configured", () => {
    const [one] = buildLinks({ ...CFG, lakebase_data_api_tab: "tables" }).filter((l) => l.group === "Lakebase");
    expect(one.title).toBe("Lakebase project (Tables · SQL editor · Data API tabs)");
    const two = buildLinks(CFG).filter((l) => l.group === "Lakebase");
    expect(two.map((l) => [l.title, l.url])).toEqual([["Lakebase project (Tables)", `${LB}/tables`],
      ["Data API settings", `${LB}/data-api`]]);
    expect(lakebaseUrl({ ...CFG, lakebase_branch_uid: null })).toBeNull();
  });

  it("shows the configured schedule on the daily job tile", () => {
    expect(buildLinks(CFG).find((l) => l.id === "daily")!.detail).toBe("hh_portal_daily - 0 0 2 * * ? (UTC)");
    expect(buildLinks({ ...CFG, daily_schedule: null }).find((l) => l.id === "daily")!.detail).toBe("hh_portal_daily");
  });

  it("leaves a link empty when an identifier is missing", () => {
    const by = Object.fromEntries(buildLinks({ ...CFG, sync_pipeline_id: null, host: CFG.host }).map((l) => [l.id, l.url]));
    expect(by.pipeline).toBeNull();
    expect(buildLinks({ ...CFG, host: null }).filter((l) => l.group !== "Source").every((l) => l.url === null)).toBe(true);
    expect(repoFile(null, "docs/ADR.md")).toBeNull();
  });
});
