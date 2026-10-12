// "Open in Databricks" links for the Architecture page, built only from bundle-supplied identifiers.

export interface ArchConfig {
  host: string | null; catalog: string | null; data_schema: string | null; synced_schema: string;
  gold_schema: string; sync_pipeline_id: string | null; daily_job_id: string | null; daily_schedule: string | null;
  setup_job_id: string | null; lakebase_project: string | null; lakebase_branch: string | null; repo_url: string | null;
  lakebase_project_uid: string | null; lakebase_branch_uid: string | null; lakebase_data_api_tab: string;
  data_api_url: string | null;
}

export interface LinkCard { id: string; group: "Unity Catalog" | "Lakeflow" | "Lakebase" | "Source"; title: string;
  detail: string; url: string | null }

const seg = (s: string) => encodeURIComponent(s);

function join(host: string | null, ...parts: (string | null)[]): string | null {
  if (!host || parts.some((p) => !p)) return null;
  return `${host}/${parts.map((p) => p!.split("/").map(seg).join("/")).join("/")}`;
}

/** GitHub blob URL for a doc next to the source tree link (tree/<ref>/<dir> -> blob/<ref>/<dir>/<file>). */
export function repoFile(repoUrl: string | null, file: string, anchor = ""): string | null {
  if (!repoUrl) return null;
  return `${repoUrl.replace("/tree/", "/blob/").replace(/\/$/, "")}/${file}${anchor}`;
}

/** Lakebase project UI: {host}/lakebase/projects/<project uid>/branches/<branch uid>/<tab> (tabs "tables" and
 *  "data-api" confirmed). */
export function lakebaseUrl(c: ArchConfig, tab = "tables"): string | null {
  return join(c.host, "lakebase/projects", c.lakebase_project_uid, "branches", c.lakebase_branch_uid, tab);
}

function lakebaseCards(c: ArchConfig): LinkCard[] {
  const tab = c.lakebase_data_api_tab || "data-api";
  if (tab === "tables") {
    return [{ id: "project", group: "Lakebase", title: "Lakebase project (Tables · SQL editor · Data API tabs)",
      url: lakebaseUrl(c), detail: `${c.lakebase_project} / ${c.lakebase_branch}: portal, portal_work and portal_gold` }];
  }
  return [
    { id: "project", group: "Lakebase", title: "Lakebase project (Tables)", url: lakebaseUrl(c),
      detail: `${c.lakebase_project} / ${c.lakebase_branch}: portal, portal_work and portal_gold` },
    { id: "data-api", group: "Lakebase", title: "Data API settings", url: lakebaseUrl(c, tab),
      detail: "Exposed schemas (portal_gold), max rows, Refresh schema cache" },
  ];
}

export function buildLinks(c: ArchConfig): LinkCard[] {
  const table = (schema: string | null, name: string) => join(c.host, "explore/data", c.catalog, schema, name);
  return [
    { id: "silver", group: "Unity Catalog", title: "Silver table", url: table(c.data_schema, "silver_hh_readings"),
      detail: `${c.catalog}.${c.data_schema}.silver_hh_readings - 13 months, Change Data Feed on` },
    { id: "synced", group: "Unity Catalog", title: "Synced table (online view)",
      url: table(c.synced_schema, "silver_hh_readings"), detail: `${c.catalog}.${c.synced_schema}.silver_hh_readings` },
    { id: "log", group: "Unity Catalog", title: "Refresh log", url: table(c.data_schema, "portal_refresh_log"),
      detail: "Timings and row counts of every daily step" },
    { id: "pipeline", group: "Lakeflow", title: "Sync pipeline", url: join(c.host, "pipelines", c.sync_pipeline_id),
      detail: "TRIGGERED synced-table pipeline, started by the daily job" },
    { id: "daily", group: "Lakeflow", title: "Daily refresh job", url: join(c.host, "jobs", c.daily_job_id),
      detail: `hh_portal_daily${c.daily_schedule ? ` - ${c.daily_schedule}` : ""}` },
    { id: "setup", group: "Lakeflow", title: "Setup job", url: join(c.host, "jobs", c.setup_job_id),
      detail: "hh_portal_setup - generates the synthetic data and silver" },
    ...lakebaseCards(c),
    { id: "code", group: "Source", title: "Source code", url: c.repo_url, detail: "app, jobs, gold SQL, provisioning" },
    { id: "adr", group: "Source", title: "Design decisions", url: repoFile(c.repo_url, "docs/ADR.md"),
      detail: "Why gold lives in Lakebase, refresh, roles" },
    { id: "runbook", group: "Source", title: "Runbook", url: repoFile(c.repo_url, "docs/RUNBOOK.md"),
      detail: "Ordered provisioning steps, durations, costs" },
  ];
}
