// "Select MPANs" side panel: search, select all, accounts with their meters (checkboxes), apply.
import { useEffect, useMemo, useRef, useState } from "react";
import { filterGroups, meterAddress, resolveSelection, type AccountGroup, type Selection } from "../lib/mpans";
import type { Org } from "../types";
import { IconBolt, IconChevronDown, IconChevronUp, IconClose, IconFilter, IconSearch } from "./Icons";

function AccountCheckbox({ state, onChange, label }: { state: "all" | "some" | "none"; onChange: () => void;
  label: string }) {
  const ref = useRef<HTMLInputElement>(null);
  useEffect(() => { if (ref.current) ref.current.indeterminate = state === "some"; }, [state]);
  return <input ref={ref} type="checkbox" checked={state === "all"} onChange={onChange} aria-label={label} />;
}

export function MpanDrawer({ groups, org, allowOrg, initial, onApply, onClose }: { groups: AccountGroup[]; org: Org;
  allowOrg: boolean; initial: ReadonlySet<string>; onApply: (s: Selection, mpans: Set<string>) => void;
  onClose: () => void }) {
  const [selected, setSelected] = useState<Set<string>>(() => new Set(initial));
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState<Set<string>>(() => new Set(groups.slice(0, 1).map((g) => g.account.account_id)));
  const panel = useRef<HTMLDivElement>(null);
  const shown = useMemo(() => filterGroups(groups, query), [groups, query]);
  const all = useMemo(() => groups.flatMap((g) => g.meters.map((m) => m.mpan)), [groups]);
  const result = resolveSelection(selected, groups, org, allowOrg);

  useEffect(() => {
    panel.current?.querySelector<HTMLInputElement>("input[type=search]")?.focus();
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  const toggle = (ids: string[], on: boolean) => setSelected((s) => {
    const n = new Set(s);
    ids.forEach((id) => (on ? n.add(id) : n.delete(id)));
    return n;
  });
  const flip = (id: string) => setOpen((s) => {
    const n = new Set(s);
    if (n.has(id)) n.delete(id); else n.add(id);
    return n;
  });

  return (
    <div className="drawer-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="drawer" role="dialog" aria-modal="true" aria-labelledby="drawer-title" ref={panel}>
        <div className="drawer-head">
          <h2 id="drawer-title"><IconFilter /> Select MPANs</h2>
          <button type="button" className="icon-button round" onClick={onClose} aria-label="Close"><IconClose /></button>
        </div>
        <label className="search">
          <span className="sr-only">Search MPANs</span>
          <input type="search" value={query} onChange={(e) => setQuery(e.target.value)}
            placeholder="Search by entering address, postcode, account..." />
          <IconSearch />
        </label>
        <div className="drawer-bar">
          <button type="button" className="link-button" onClick={() => setSelected(selected.size === all.length ? new Set()
            : new Set(all))}>{selected.size === all.length ? "Clear selection" : "Select all MPANs"}</button>
          <span className="subtle" aria-live="polite">{selected.size} MPAN{selected.size === 1 ? "" : "s"} selected</span>
        </div>
        <div className="drawer-list">
          {shown.length === 0 && <p className="subtle">No MPAN matches "{query}".</p>}
          {shown.map((g) => {
            const ids = g.meters.map((m) => m.mpan);
            const n = ids.filter((id) => selected.has(id)).length;
            const state = n === 0 ? "none" : n === ids.length ? "all" : "some";
            const isOpen = open.has(g.account.account_id) || query.trim() !== "";
            return (
              <div key={g.account.account_id} className={`acct${isOpen ? " open" : ""}`}>
                <div className="acct-row">
                  <AccountCheckbox state={state} onChange={() => toggle(ids, state !== "all")}
                    label={`Select all MPANs of ${g.account.account_name}`} />
                  <div className="acct-text">
                    <div className="acct-name">{g.account.account_name}{g.address ? `, ${g.address}` : ""}</div>
                    <div className="acct-id">IST {g.account.account_id} · {ids.length} MPAN{ids.length === 1 ? "" : "s"}</div>
                  </div>
                  <span className="acct-bolt"><IconBolt /></span>
                  <button type="button" className="icon-button" aria-expanded={isOpen}
                    aria-label={`${isOpen ? "Hide" : "Show"} MPANs of ${g.account.account_name}`}
                    onClick={() => flip(g.account.account_id)}>{isOpen ? <IconChevronUp /> : <IconChevronDown />}</button>
                </div>
                {isOpen && (
                  <ul className="meter-list">
                    {g.meters.map((m) => (
                      <li key={m.mpan}>
                        <label>
                          <input type="checkbox" checked={selected.has(m.mpan)}
                            onChange={(e) => toggle([m.mpan], e.target.checked)} />
                          <span>
                            <span className="meter-id">MPAN {m.mpan}</span>
                            <span className="meter-address">{meterAddress(m)}</span>
                          </span>
                        </label>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            );
          })}
        </div>
        <div className="drawer-foot">
          <span className={"error" in result ? "error-text" : "subtle"}>
            {"error" in result ? result.error : result.scope === "org" ? "Organisation totals (all MPANs)"
              : result.scope === "account" ? "Account totals" : "Selected MPANs"}
          </span>
          <button type="button" className="btn btn-navy" disabled={"error" in result}
            onClick={() => { if (!("error" in result)) onApply(result, selected); }}>Show data</button>
        </div>
      </div>
    </div>
  );
}
