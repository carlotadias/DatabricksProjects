import { describe, expect, it } from "vitest";
import { filterOrgs, hrefFor, pageFromHash, pageLabel } from "./nav";

describe("navigation", () => {
  it("maps the hash to a page and defaults to half-hourly data", () => {
    expect(pageFromHash("")).toBe("half-hourly");
    expect(pageFromHash("#/invoices")).toBe("invoices");
    expect(pageFromHash("#/architecture")).toBe("architecture");
    expect(pageFromHash("#energy-reports")).toBe("energy-reports");
    expect(pageFromHash("#/unknown")).toBe("half-hourly");
    expect(pageFromHash("#main")).toBe("half-hourly");
    expect(hrefFor("contract-tariff")).toBe("#/contract-tariff");
    expect(pageLabel("meter-readings")).toBe("Meter readings");
  });

  it("filters and orders organisations for the switcher", () => {
    const orgs = [{ org_id: "88000002", org_name: "Birch Care", is_tpi: false },
      { org_id: "88000001", org_name: "Alder Foods", is_tpi: false }, { org_id: "88000003", org_name: "Maple Group", is_tpi: true }];
    expect(filterOrgs(orgs, "").map((o) => o.org_id)).toEqual(["88000003", "88000001", "88000002"]);
    expect(filterOrgs(orgs, "birch").map((o) => o.org_id)).toEqual(["88000002"]);
    expect(filterOrgs(orgs, "000001").map((o) => o.org_id)).toEqual(["88000001"]);
  });
});
