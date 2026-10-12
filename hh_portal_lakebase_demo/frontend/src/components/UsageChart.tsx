// Bar chart of consumption (kWh) or cost (GBP: consumption + standing charge stacked), hover tooltip and a
// dashed average line. The same rows are available as a table ("Show as a table").
import { useState } from "react";
import { Bar, BarChart, CartesianGrid, Cell, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmtDayMonth, fmtDayTime, fmtGbp, fmtKwh, fmtMonthYear, fmtTime } from "../lib/format";
import type { Bar as Row, Granularity } from "../lib/periods";

export type Unit = "kwh" | "gbp";

const COLORS = { bar: "var(--teal-600)", active: "var(--teal-800)", standing: "var(--standing)" };

function xLabel(t: string, g: Granularity): string {
  if (g === "halfhour" || g === "hour") return fmtTime(t);
  if (g === "day") return fmtDayMonth(t).slice(0, 2);
  return fmtMonthYear(t).slice(0, 3);
}

export function tooltipTitle(t: string, g: Granularity): string {
  if (g === "halfhour" || g === "hour") return fmtDayTime(t);
  if (g === "day") return fmtDayMonth(t);
  return fmtMonthYear(t);
}

interface TipProps { active?: boolean; payload?: { payload: Row & { label: string } }[]; unit: Unit; g: Granularity }

function Tip({ active, payload, unit, g }: TipProps) {
  if (!active || !payload?.length) return null;
  const r = payload[0].payload;
  return (
    <div className="chart-tip" role="status">
      <div>{tooltipTitle(r.t, g)}</div>
      {r.missing ? <div>No readings yet</div> : unit === "kwh" ? (
        <div>Consumption: {fmtKwh(r.kwh)} kWh</div>
      ) : (
        <>
          <div>Consumption: {fmtGbp(r.consumption_gbp)}</div>
          <div>Standing charge: {fmtGbp(r.standing_gbp)}</div>
          <div>Total: {fmtGbp(r.total_gbp)}</div>
        </>
      )}
    </div>
  );
}

export function average(rows: Row[], unit: Unit): number | null {
  const real = rows.filter((r) => !r.missing);
  if (!real.length) return null;
  const sum = real.reduce((s, r) => s + (unit === "kwh" ? r.kwh : r.total_gbp), 0);
  return sum / real.length;
}

export function UsageChart({ rows, granularity, unit, label }: { rows: Row[]; granularity: Granularity;
  unit: Unit; label: string }) {
  const [active, setActive] = useState<number | null>(null);
  const data = rows.map((r) => ({ ...r, label: xLabel(r.t, granularity) }));
  const avg = average(rows, unit);
  const interval = granularity === "halfhour" ? 3 : granularity === "hour" ? 2 : rows.length > 16 ? 1 : 0;
  const avgText = avg === null ? "" : unit === "kwh" ? `Average ${fmtKwh(avg)} kWh` : `Average ${fmtGbp(avg)}`;
  return (
    <figure className="chart" aria-label={label}>
      <div className="chart-legend" aria-hidden="true">
        <span><i className="dot dot-consumption" />Consumption</span>
        {unit === "gbp" && <span><i className="dot dot-standing" />Standing charge</span>}
      </div>
      <div className="chart-box">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data} margin={{ top: 16, right: 8, bottom: 0, left: -8 }} barCategoryGap="18%" maxBarSize={56}
            onMouseMove={(s) => setActive(typeof s?.activeTooltipIndex === "number" ? s.activeTooltipIndex : null)}
            onMouseLeave={() => setActive(null)}>
            <CartesianGrid vertical={false} stroke="var(--grid)" />
            <XAxis dataKey="label" tickLine={false} axisLine={{ stroke: "var(--grid)" }} interval={interval}
              tick={{ fill: "var(--text-muted)", fontSize: 11 }} />
            <YAxis tickLine={false} axisLine={false} width={48} tick={{ fill: "var(--text-muted)", fontSize: 11 }}
              tickFormatter={(v: number) => (unit === "gbp" ? `£${v.toFixed(v < 10 ? 2 : 0)}` : v.toFixed(v < 10 ? 2 : 0))} />
            <Tooltip cursor={false} content={<Tip unit={unit} g={granularity} />} isAnimationActive={false} />
            {unit === "gbp" && (
              <Bar dataKey="standing_gbp" stackId="c" fill={COLORS.standing} isAnimationActive={false} />
            )}
            <Bar dataKey={unit === "kwh" ? "kwh" : "consumption_gbp"} stackId="c" radius={[3, 3, 0, 0]}
              isAnimationActive={false}>
              {data.map((_, i) => <Cell key={i} fill={i === active ? COLORS.active : COLORS.bar} />)}
            </Bar>
            {avg !== null && (
              <ReferenceLine y={avg} stroke="var(--text-muted)" strokeDasharray="3 3"
                label={{ value: avgText, position: "insideTopRight", fill: "var(--text-muted)", fontSize: 11 }} />
            )}
          </BarChart>
        </ResponsiveContainer>
      </div>
      <figcaption className="sr-only">{label}. {avgText}. Use "Show as a table" for every value.</figcaption>
    </figure>
  );
}
