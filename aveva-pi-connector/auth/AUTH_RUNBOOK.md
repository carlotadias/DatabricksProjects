# Authentication feasibility runbook — AVEVA PI Web API

**Goal:** find out — in minutes, before building anything — whether a Databricks
cluster can authenticate to your PI Web API with **HTTP Basic** (the scheme this
connector uses). `basic_auth_probe.py` is a **feasibility probe**, not a deployment:
it changes nothing on your PI/AD side, it only *tries* to authenticate and reports
where it stops.

> **What the connector implements: Basic.** Bearer/OIDC is the long-term ideal
> (once PI is 2023+) but out of scope for this build; direct Kerberos-from-Spark is
> deliberately **not** supported (fragile — keytab + KDC egress + renewal on every
> executor). If Basic can't reach PI, the **in-domain proxy** is the near-term
> unblock: a domain-joined box does the hard auth to PI and exposes a Basic-speaking
> endpoint — point `endpoint_url` at it and keep using Basic. The full trade-off is
> in [README §Authentication](../README.md#authentication).

---

## Run `basic_auth_probe.py`

**Why Basic:** it's just an `Authorization: Basic` header — no keytab, no KDC, no
init script, runs on **any** cluster (classic or serverless), and the connector
supports it natively (`basic_user` / `basic_password`, sent pre-emptively).

**What you need:**
- a **read-only AD service account** (username + password, ASCII password) — PI
  Web API stores no credentials of its own; it delegates identity to Windows/AD,
  so this account comes from the customer's **AD / identity team**.
- PI's **FQDN**, and the **CA cert** if PI uses an internal CA.
- credentials in a Databricks **secret scope** (never hardcode).

**How to run:** upload `basic_auth_probe.py`, fill in the first cell (endpoint,
scope, secret keys), **Run all**, read the Summary.

## Reading the result

Step 2 reads PI's `WWW-Authenticate` header on an anonymous request — treat that as
a **hint, not a verdict**. The header only *advertises* what the server offers
unprompted; it is **not** the definitive list of schemes the server will *accept*.
A server can advertise only `Negotiate` yet still honour a Basic `Authorization`
header sent pre-emptively (and vice-versa). So:

- **`Basic` listed** → good sign; if step 3 (which actually *sends* Basic) returns
  200, you're done — the simplest path (InfoSec sign-off still needed).
- **only `Negotiate`/`NTLM`** → Basic *may* still be disabled by group policy
  (typical in a Kerberos-first estate) — **but don't conclude that from the header
  alone.** Let step 3 send Basic anyway and check the result. Only if that also
  fails is Basic actually off, and the **in-domain proxy** becomes the route.

> ⚠️ **The on-box test is not the remote test.** Basic returning 200 from `curl` on
> the PI VM (`localhost`) does **not** prove it works from Databricks over the FQDN
> — a proxy/GPO can allow it locally and block it remotely. Run step 3 *from the
> cluster* against the FQDN to settle it. This is the one thing still worth proving.

> Even where Basic works, confirm with InfoSec — it sends a reusable AD credential
> on every request (over TLS). A dedicated least-privilege account is essential.

## Requirements to run
- Any cluster with a **network path to PI (443)**.
- **DBR 15.x+**; the probe only needs `requests` (already present).
- The Basic credentials in a secret scope the cluster can read.
