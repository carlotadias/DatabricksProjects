// Parse the single versioned gold SQL file (sql/lakebase_gold.sql, bundled with ?raw) into one entry per
// view / materialized view with its unique and lookup indexes and its refresh statement. Mirrors src/hhportal_data/gold.py.

export interface GoldObject {
  schema: string; name: string; kind: "VIEW" | "MATERIALIZED VIEW"; create: string; indexes: string[];
  refresh: string | null; exposed: boolean;
}

export function splitStatements(text: string): string[] {
  const body = text.split("\n").filter((l) => !l.trimStart().startsWith("--")).join("\n");
  return body.split(/;[ \t]*$/m).map((s) => s.trim()).filter(Boolean);
}

const CREATE = /^CREATE (MATERIALIZED VIEW IF NOT EXISTS|OR REPLACE VIEW) (\w+)\.(\w+) AS/;
const INDEX = /^CREATE (?:UNIQUE )?INDEX IF NOT EXISTS \w+ ON (\w+)\.(\w+) /;

export function parseGold(text: string): GoldObject[] {
  const out: GoldObject[] = [];
  for (const s of splitStatements(text)) {
    const m = CREATE.exec(s);
    if (m) {
      const mv = m[1].startsWith("MATERIALIZED");
      const exposed = m[2] === "portal_gold";
      out.push({ schema: m[2], name: m[3], kind: mv ? "MATERIALIZED VIEW" : "VIEW", create: `${s};`, indexes: [],
        refresh: mv ? `REFRESH MATERIALIZED VIEW ${exposed ? "CONCURRENTLY " : ""}${m[2]}.${m[3]};` : null, exposed });
      continue;
    }
    const i = INDEX.exec(s);
    if (i) out.find((o) => o.schema === i[1] && o.name === i[2])?.indexes.push(`${s};`);
  }
  return out;
}

export function fullSql(o: GoldObject): string {
  return [o.create, ...o.indexes, ...(o.refresh ? [`-- nightly (hh_portal_daily refresh_gold):\n${o.refresh}`] : [])]
    .join("\n\n");
}

const KEYWORDS = new Set(("SELECT FROM WHERE JOIN LEFT ON AND OR AS GROUP BY ORDER WITH CREATE MATERIALIZED VIEW REPLACE " +
  "INDEX UNIQUE IF NOT EXISTS DATA REFRESH CONCURRENTLY DISTINCT NULL IS DESC ASC FILTER COUNT SUM MIN MAX " +
  "CASE WHEN THEN ELSE END").split(" "));

/** Light highlighting: SQL keywords and comments as separate tokens; everything else verbatim. */
export function highlightSql(text: string): { t: string; k?: "kw" | "comment" }[] {
  const out: { t: string; k?: "kw" | "comment" }[] = [];
  for (const part of text.split(/(--[^\n]*|\b[A-Za-z_]+\b)/)) {
    if (!part) continue;
    if (part.startsWith("--")) out.push({ t: part, k: "comment" });
    else if (KEYWORDS.has(part)) out.push({ t: part, k: "kw" });
    else out.push({ t: part });
  }
  return out;
}
