// Bottom-left organisation switcher: a button that opens a small searchable menu of demo customers.
import { useEffect, useRef, useState } from "react";
import { filterOrgs } from "../lib/nav";
import type { Org } from "../types";
import { IconBuilding, IconChevronDown, IconSearch } from "./Icons";

export function OrgSwitcher({ orgs, value, onChange }: { orgs: Org[]; value: Org | null; onChange: (id: string) => void }) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const root = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  const shown = filterOrgs(orgs, q);

  useEffect(() => {
    if (!open) return;
    root.current?.querySelector<HTMLInputElement>("input")?.focus();
    const onDown = (e: MouseEvent) => { if (!root.current?.contains(e.target as Node)) setOpen(false); };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") { setOpen(false); button.current?.focus(); }
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => { document.removeEventListener("mousedown", onDown); document.removeEventListener("keydown", onKey); };
  }, [open]);

  const pick = (id: string) => { onChange(id); setOpen(false); setQ(""); button.current?.focus(); };
  const sites = value ? `${value.sites} site${value.sites === 1 ? "" : "s"}${value.is_tpi ? " · TPI" : ""}` : "";

  return (
    <div className="org-switcher-wrap" ref={root}>
      {open && (
        <div className="org-menu" role="dialog" aria-label="Choose an organisation">
          <label className="search">
            <span className="sr-only">Search organisations</span>
            <input type="search" value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search name or 88 number" />
            <IconSearch />
          </label>
          <ul role="listbox" aria-label="Organisations">
            {shown.length === 0 && <li className="subtle org-none">No organisation matches.</li>}
            {shown.map((o) => (
              <li key={o.org_id} role="option" aria-selected={o.org_id === value?.org_id}>
                <button type="button" className={o.org_id === value?.org_id ? "current" : ""} onClick={() => pick(o.org_id)}>
                  <span className="org-name">{o.org_name}</span>
                  <span className="org-sites">{o.org_id} · {o.sites} site{o.sites === 1 ? "" : "s"}{o.is_tpi ? " · TPI" : ""}</span>
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}
      <button type="button" ref={button} className="org-switcher" aria-haspopup="dialog" aria-expanded={open}
        onClick={() => setOpen((x) => !x)} title={value ? `${value.org_name} - ${sites}` : "Choose an organisation"}>
        <span className="org-icon" aria-hidden="true"><IconBuilding /></span>
        <span className="org-text">
          <span className="org-name">{value?.org_name ?? "Choose a customer"}</span>
          <span className="org-sites">{sites}</span>
        </span>
        <span className="org-chevron"><IconChevronDown /></span>
        <span className="sr-only">Change organisation</span>
      </button>
    </div>
  );
}
