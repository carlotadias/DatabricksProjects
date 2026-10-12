// "How the app calls the Data API": the request, a Python httpx snippet mirroring app/hhportal/dataapi.py,
// and a curl equivalent. Placeholders only; never a real token.

export const BASE_PLACEHOLDER = "<DATA_API_URL>";

/** Postgres database of the Data API URL (.../rest/<database>), default databricks_postgres. */
export function postgresDatabase(dataApiUrl: string | null): string {
  return /\/rest\/([A-Za-z0-9_]+)\/?$/.exec(dataApiUrl ?? "")?.[1] ?? "databricks_postgres";
}

export function dailyMpanPath(mpan: string, d1: string, d2: string): string {
  return `/portal_gold/daily_mpan?select=reading_date,kwh,total_gbp&mpan=eq.${mpan}` +
    `&reading_date=gte.${d1}&reading_date=lte.${d2}&order=reading_date.asc`;
}

export function dataApiSnippets(base: string | null, mpan: string, d1: string, d2: string) {
  const url = base ?? BASE_PLACEHOLDER;
  const path = dailyMpanPath(mpan, d1, d2);
  const http = `GET ${url}${path}\nAuthorization: Bearer <OAuth token>\nAccept: application/json`;
  const python = [
    "import httpx",
    "from databricks.sdk import WorkspaceClient",
    "",
    `DATA_API_URL = "${url}"`,
    "headers = WorkspaceClient().config.authenticate()  # {'Authorization': 'Bearer <OAuth token>'}",
    `params = [("select", "reading_date,kwh,total_gbp"), ("mpan", "eq.${mpan}"),`,
    `          ("reading_date", "gte.${d1}"), ("reading_date", "lte.${d2}"), ("order", "reading_date.asc")]`,
    'r = httpx.get(f"{DATA_API_URL}/portal_gold/daily_mpan", params=params, headers=headers, timeout=30)',
    "r.raise_for_status()",
    "for row in r.json():  # a JSON array, at most db_max_rows (2000) rows per request",
    '    print(row["reading_date"], row["kwh"], row["total_gbp"])',
  ].join("\n");
  const curl = [
    "# your identity needs a Postgres role created with databricks_create_role and granted to authenticator",
    "TOKEN=$(databricks auth token -p <profile> | jq -r .access_token)",
    `curl -s -H "Authorization: Bearer $TOKEN" \\\n  "${url}${path}"`,
  ].join("\n");
  return { http, python, curl, path };
}
