// App shell: sidebar menu (customer), top bar with the demo persona switch and the signed-in user.
// Sidebar: full width >= 1024 px, icon rail 768-1023 px, hamburger drawer below 768 px.
import { useEffect, useState, type ReactNode } from "react";
import { api, useLoad } from "./api";
import { ChartSkeleton, Empty, ErrorNote, Segmented } from "./components/Bits";
import {
  IconArchitecture, IconClose, IconContract, IconHalfHourly, IconInvoice, IconMenu, IconMeter, IconOverview, IconReport, IconUser,
} from "./components/Icons";
import { OrgSwitcher } from "./components/OrgSwitcher";
import { HOME, hrefFor, pageFromHash, pageLabel, PAGES, type PageId } from "./lib/nav";
import type { Bounds } from "./lib/periods";
import { AdvisorPage } from "./pages/AdvisorPage";
import { ArchitecturePage } from "./pages/ArchitecturePage";
import { CustomerPage } from "./pages/CustomerPage";

type Persona = "customer" | "advisor";

const ICONS: Record<PageId, ReactNode> = {
  "overview": <IconOverview />, "meter-readings": <IconMeter />, "invoices": <IconInvoice />,
  "energy-reports": <IconReport />, "contract-tariff": <IconContract />, "half-hourly": <IconHalfHourly />,
  "architecture": <IconArchitecture />,
};

function displayName(identity: string | null | undefined): string {
  if (!identity) return "Signed in";
  const local = identity.split("@")[0];
  return local.split(/[._-]+/).filter(Boolean).map((w) => w[0].toUpperCase() + w.slice(1)).join(" ");
}

function Brand() {
  return (
    <div className="brand">
      <span className="brand-mark" aria-hidden="true"><IconHalfHourly /></span>
      <span className="brand-text">Energy portal <span className="brand-sub">(demo)</span></span>
    </div>
  );
}

function usePage(): PageId {
  const [page, setPage] = useState<PageId>(() => pageFromHash(window.location.hash));
  useEffect(() => {
    const on = () => setPage(pageFromHash(window.location.hash));
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);
  return page;
}

function Placeholder({ page }: { page: PageId }) {
  return (
    <>
      <div className="page-head"><h1>{pageLabel(page)}</h1></div>
      <section className="card placeholder">
        <Empty>
          <p><strong>Not part of this demo</strong> - this demo covers half-hourly consumption data.</p>
          <a className="btn btn-navy" href={hrefFor(HOME)}>Go to Half-hourly data</a>
        </Empty>
      </section>
    </>
  );
}

export default function App() {
  const me = useLoad("me", api.me);
  const fresh = useLoad("freshness", api.freshness);
  const customers = useLoad("customers", api.customers);
  const page = usePage();
  const [persona, setPersona] = useState<Persona>("customer");
  const [navOpen, setNavOpen] = useState(false);
  const [orgId, setOrgId] = useState<string | null>(null);
  const orgs = customers.data?.customers ?? [];
  const org = orgs.find((o) => o.org_id === orgId) ?? orgs.find((o) => o.is_tpi) ?? orgs[0] ?? null;
  const onHalfHourly = persona === "customer" && page === "half-hourly";
  const customer = useLoad(onHalfHourly && org ? `customer:${org.org_id}` : null, () => api.customer(org!.org_id));
  const bounds: Bounds | null = fresh.data?.first_date && fresh.data?.last_date
    ? { first: fresh.data.first_date, last: fresh.data.last_date } : null;
  const identity = me.data?.identity ?? null;

  useEffect(() => {
    if (!navOpen) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setNavOpen(false); };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [navOpen]);

  let content;
  if (persona === "customer" && page === "architecture") {
    content = <ArchitecturePage />;
  } else if (persona === "customer" && page !== "half-hourly") {
    content = <Placeholder page={page} />;
  } else if (me.data && !me.data.data_api_configured) {
    content = <ErrorNote error={new Error("The Data API URL is not configured for this app (bundle variable lakebase_data_api_url).")} />;
  } else if (fresh.error) {
    content = <ErrorNote error={fresh.error} />;
  } else if (!bounds) {
    content = <ChartSkeleton />;
  } else if (persona === "advisor") {
    content = <AdvisorPage bounds={bounds} identity={identity} />;
  } else if (customer.error || customers.error) {
    content = <ErrorNote error={(customer.error ?? customers.error)!} />;
  } else if (!customer.data) {
    content = <ChartSkeleton />;
  } else {
    content = <CustomerPage key={customer.data.org.org_id} customer={customer.data} bounds={bounds} />;
  }

  const focusMain = () => document.getElementById("main")?.focus();

  return (
    <div className={`shell persona-${persona}${navOpen ? " nav-open" : ""}`}>
      <button type="button" className="skip-link" onClick={focusMain}>Skip to content</button>
      {persona === "customer" && (
        <>
          <div className="nav-backdrop" aria-hidden="true" onClick={() => setNavOpen(false)} />
          <aside className="sidebar" id="portal-menu" aria-label="Portal menu">
            <div className="sidebar-head">
              <Brand />
              <button type="button" className="icon-button nav-close" aria-label="Close menu" onClick={() => setNavOpen(false)}>
                <IconClose /></button>
            </div>
            <nav aria-label="Main">
              <p className="menu-label">Menu</p>
              <ul>
                {PAGES.map((p) => (
                  <li key={p.id}>
                    <a className={`menu-item${p.id === page ? " active" : ""}`} href={hrefFor(p.id)} title={p.label}
                      aria-current={p.id === page ? "page" : undefined} onClick={() => setNavOpen(false)}>
                      {ICONS[p.id]}<span className="menu-text">{p.label}</span>
                    </a>
                  </li>
                ))}
              </ul>
            </nav>
            <div className="sidebar-foot">
              <OrgSwitcher orgs={orgs} value={org} onChange={(id) => { setOrgId(id); setNavOpen(false); window.location.hash = hrefFor(HOME); }} />
            </div>
          </aside>
        </>
      )}
      <div className="main-col">
        <header className="topbar">
          {persona === "customer" && (
            <button type="button" className="icon-button nav-toggle" aria-label="Open menu" aria-expanded={navOpen}
              aria-controls="portal-menu" onClick={() => setNavOpen(true)}><IconMenu /></button>
          )}
          {persona === "advisor" ? <Brand /> : <span className="topbar-spacer" />}
          <div className="topbar-right">
            <span className="badge-synthetic" title="All data in this demo is synthetic">Synthetic data</span>
            <Segmented small label="Demo view" value={persona} onChange={(p) => { setPersona(p); setNavOpen(false); }}
              options={[{ id: "customer", label: "Customer" }, { id: "advisor", label: "Advisor" }]} />
            <span className="user"><span className="user-name">{displayName(identity)}</span>
              <span className="avatar"><IconUser /></span></span>
          </div>
        </header>
        <main id="main" className="page" tabIndex={-1}>{content}</main>
      </div>
    </div>
  );
}
