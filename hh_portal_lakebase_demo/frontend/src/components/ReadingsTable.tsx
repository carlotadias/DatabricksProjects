// Table of the bars currently shown (also the accessible alternative to the chart) with CSV download.
import { csvFilename, downloadCsv, toCsv } from "../lib/csv";
import { fmtGbp, fmtKwh, fmtReadingStamp } from "../lib/format";
import type { Granularity } from "../lib/periods";
import type { UsageRow } from "../types";

export function readingsCsv(rows: UsageRow[]): string {
  return toCsv(["timestamp", "usage_kwh", "consumption_gbp", "standing_charge_gbp", "total_gbp"],
    rows.map((r) => [r.t, r.kwh, r.consumption_gbp, r.standing_gbp, r.total_gbp]));
}

export function download(rows: UsageRow[], ...nameParts: string[]): void {
  downloadCsv(csvFilename("energy-usage", ...nameParts), readingsCsv(rows));
}

export function ReadingsTable({ rows, granularity, caption, title = "Readings", onDownload }: { rows: UsageRow[];
  granularity: Granularity; caption: string; title?: string; onDownload: () => void }) {
  const perSlot = granularity === "halfhour" || granularity === "hour"; // per reading: consumption cost only
  const noun = granularity === "halfhour" ? "Readings" : granularity === "hour" ? "Hours" : granularity === "day"
    ? "Days" : "Months";
  return (
    <section className="card readings" aria-labelledby="readings-title">
      <div className="readings-head">
        <h2 id="readings-title">{title}</h2>
        <span className="subtle">{caption} · {rows.length} {noun}</span>
        <button type="button" className="btn btn-outline" onClick={onDownload} disabled={!rows.length}>Download</button>
      </div>
      <div className="table-scroll" tabIndex={0} aria-label={`${title} table`}>
        <table>
          <thead>
            <tr><th scope="col">Timestamp</th><th scope="col" className="num">Usage (kWh)</th>
              <th scope="col" className="num">{perSlot ? "Cost (GBP)" : "Cost incl. standing (GBP)"}</th></tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.t}>
                <td>{fmtReadingStamp(r.t, granularity)}</td>
                <td className="num">{fmtKwh(r.kwh)}</td>
                <td className="num">{fmtGbp(perSlot ? r.consumption_gbp : r.total_gbp)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
