import { describe, expect, it } from "vitest";
import { changeChip, pctChange } from "./compare";
import { csvFilename, toCsv } from "./csv";
import { fmtDayTime, fmtGbp, fmtKwh, fmtReadingStamp, fmtTime } from "./format";
import { defaultSelection, filterGroups, groupMeters, MAX_MPANS, resolveSelection } from "./mpans";
import type { Account, Meter, Org } from "../types";

describe("vs previous", () => {
  it("computes the change and the chip text", () => {
    expect(pctChange(112, 100)).toBeCloseTo(12);
    expect(pctChange(5, 0)).toBeNull();
    expect(changeChip(pctChange(112, 100), "previous day")).toEqual({ text: "12% vs previous day", direction: "up" });
    expect(changeChip(pctChange(80, 100), "previous week")).toEqual({ text: "20% vs previous week", direction: "down" });
    expect(changeChip(pctChange(100.2, 100), "previous month")?.direction).toBe("flat");
    expect(changeChip(null, "previous day")).toBeNull();
  });
});

describe("csv", () => {
  it("quotes, rounds and neutralises formula-like cells", () => {
    const text = toCsv(["timestamp", "usage_kwh", "note"], [["2026-10-01T00:30:00+00:00", 0.123456, 'a "b", c'],
      ["x", 1, "=SUM(A1)"]]);
    expect(text).toBe('timestamp,usage_kwh,note\r\n2026-10-01T00:30:00+00:00,0.1235,"a ""b"", c"\r\nx,1,"=SUM(A1)"\r\n');
    expect(csvFilename("hh", "Alder Foods Ltd", "2026-10-01")).toBe("hh_Alder-Foods-Ltd_2026-10-01.csv");
  });
});

describe("format", () => {
  it("formats energy, money and UTC clock times", () => {
    expect(fmtKwh(6.384)).toBe("6.38");
    expect(fmtKwh(12345.6)).toBe("12,346");
    expect(fmtGbp(3.014)).toBe("£3.01");
    expect(fmtTime("2026-08-01T15:00:00+00:00")).toBe("15:00");
    expect(fmtDayTime("2026-08-01T15:00:00+00:00")).toBe("01 Aug 15:00");
    expect(fmtReadingStamp("2026-08-01T23:30:00+00:00", "halfhour")).toBe("01 AUG 23:30");
    expect(fmtReadingStamp("2026-08-01", "month")).toBe("AUG 2026");
  });
});

const ORG: Org = { org_id: "88000001", org_name: "Demo Org", is_tpi: true, accounts: 2, sites: 3, mpans: 3 };
const acc = (id: string, name: string): Account => ({ account_id: id, account_name: name, org_id: ORG.org_id,
  org_name: ORG.org_name, is_tpi: true, sites: 1, mpans: 1 });
const meter = (mpan: string, account: string, address: string, postcode: string): Meter => ({ mpan, account_id: account,
  address_line: address, postcode, site_id: "ST1", region: "", account_name: "", org_id: ORG.org_id, org_name: "",
  meter_type: "HH" });
const ACCOUNTS = [acc("87000001", "Depots"), acc("87000002", "Offices")];
const METERS = [meter("1000000000001", "87000001", "1 Mill Lane", "LS1 2AB"),
  meter("1000000000002", "87000001", "2 Mill Lane", "LS1 2AB"), meter("1000000000003", "87000002", "9 Park Road", "M4 5CD")];

describe("MPAN selection", () => {
  const groups = groupMeters(ACCOUNTS, METERS);

  it("groups meters by account and searches address, postcode, account and MPAN", () => {
    expect(groups.map((g) => [g.account.account_id, g.meters.length, g.address])).toEqual([
      ["87000001", 2, "1 Mill Lane, LS1 2AB"], ["87000002", 1, "9 Park Road, M4 5CD"]]);
    expect(filterGroups(groups, "m4 5cd").map((g) => g.account.account_id)).toEqual(["87000002"]);
    expect(filterGroups(groups, "2 mill")[0].meters.map((m) => m.mpan)).toEqual(["1000000000002"]);
    expect(filterGroups(groups, "offices")[0].meters).toHaveLength(1);
    expect(filterGroups(groups, "87000001")[0].meters).toHaveLength(2);
    expect(filterGroups(groups, "")).toBe(groups);
  });

  it("maps a selection to organisation, account or MPAN scope", () => {
    const all = new Set(METERS.map((m) => m.mpan));
    expect(resolveSelection(all, groups, ORG, true)).toEqual({ scope: "org", ids: ["88000001"], label: "Demo Org - all sites" });
    expect(resolveSelection(all, groups, ORG, false)).toMatchObject({ scope: "mpan", ids: [...all] });
    expect(resolveSelection(new Set(["1000000000001", "1000000000002"]), groups, ORG, true)).toEqual(
      { scope: "account", ids: ["87000001"], label: "Depots" });
    expect(resolveSelection(new Set(["1000000000003", "1000000000001"]), groups, ORG, false)).toEqual(
      { scope: "mpan", ids: ["1000000000001", "1000000000003"], label: "2 MPANs" });
    // one meter of a two-meter account is an MPAN selection labelled with its address
    expect(resolveSelection(new Set(["1000000000002"]), groups, ORG, false)).toEqual(
      { scope: "mpan", ids: ["1000000000002"], label: "2 Mill Lane, LS1 2AB" });
    // the only meter of an account is that account
    expect(resolveSelection(new Set(["1000000000003"]), groups, ORG, false)).toMatchObject({ scope: "account" });
    expect(resolveSelection(new Set(), groups, ORG, true)).toEqual({ error: "Select at least one MPAN." });
  });

  it("caps MPAN multi-select", () => {
    const many = Array.from({ length: MAX_MPANS + 1 }, (_, i) => meter(String(2000000000000 + i), "87000003", "x", "y"));
    const big = groupMeters([...ACCOUNTS, acc("87000003", "Big")], [...METERS, ...many]);
    const sel = new Set(many.slice(0, MAX_MPANS).map((m) => m.mpan).concat(["1000000000003"]));
    expect(resolveSelection(sel, big, ORG, false)).toHaveProperty("error");
  });

  it("defaults to the whole organisation for TPIs, else the first or requested account", () => {
    expect(defaultSelection(groups, true).size).toBe(3);
    expect([...defaultSelection(groups, false)]).toEqual(["1000000000001", "1000000000002"]);
    expect([...defaultSelection(groups, false, "87000002")]).toEqual(["1000000000003"]);
  });
});
