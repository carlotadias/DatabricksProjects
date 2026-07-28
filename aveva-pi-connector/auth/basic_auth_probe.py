# Databricks notebook source
# MAGIC %md
# MAGIC # Basic-auth feasibility probe — AVEVA PI Web API
# MAGIC
# MAGIC **Purpose:** determine whether the customer's PI Web API accepts **Basic auth**
# MAGIC (username + password) — and whether a Databricks cluster can use it. This is
# MAGIC the *simplest* auth path: no keytab, no KDC, no init script — Basic is just an
# MAGIC `Authorization: Basic <base64>` header, so this runs on **any** cluster
# MAGIC (classic or serverless).
# MAGIC
# MAGIC It answers two questions:
# MAGIC 1. **Is Basic advertised** by this PI node? (reads the `WWW-Authenticate`
# MAGIC    header on an anonymous request — a *hint*, not the last word: the header
# MAGIC    advertises what's offered unprompted, not everything the server accepts.)
# MAGIC 2. **Do our credentials actually work?** (a real Basic request → HTTP 200 —
# MAGIC    this is the authoritative check; trust it over the header in step 1.)
# MAGIC
# MAGIC > **Context:** in hardened, Kerberos-first Windows estates Basic is often
# MAGIC > **disabled by group policy** (GPO). If it *is* enabled and InfoSec permits it,
# MAGIC > it's by far the easiest path — the connector already sends an auth header, so
# MAGIC > no code change is needed. See [AUTH_RUNBOOK.md](AUTH_RUNBOOK.md) for
# MAGIC > the auth options overview.
# MAGIC >
# MAGIC > **Security:** Basic passes a Windows/AD identity as username+password. Use a
# MAGIC > **dedicated read-only service account**, an **ASCII password** (non-ASCII is a
# MAGIC > documented Basic limitation), and **verified TLS** — never disable cert
# MAGIC > verification in prod, or the password is exposed.

# COMMAND ----------

# ── EDIT THESE ────────────────────────────────────────────────────────────────
PI_FQDN    = "piserver.example.com"            # EDIT — PI Web API host (FQDN)
PI_URL     = f"https://{PI_FQDN}/piwebapi"
# Credentials — read from a secret scope, NEVER hardcode. Domain form usually
# needs DOMAIN\\user or user@domain; ask the AD team which their PI expects.
SCOPE      = "pi"                               # Databricks secret scope
USER_KEY   = "pi_basic_user"                    # secret key holding the username
PASS_KEY   = "pi_basic_password"                # secret key holding the password
VERIFY_TLS = True    # True in prod; or a CA-bundle path if PI uses an internal CA
# ──────────────────────────────────────────────────────────────────────────────

import requests
_results = []
def record(step, ok, note=""):
    _results.append((step, bool(ok), note))
    print(f"{'✅ PASS' if ok else '❌ FAIL'}  {step}" + (f" — {note}" if note else ""))

# COMMAND ----------

# MAGIC %md ## 1. Reachability + TLS
# MAGIC Can we reach PI at all, and does its certificate verify? An SSL error here
# MAGIC means an internal CA — set `VERIFY_TLS` to the CA-bundle path.

# COMMAND ----------

try:
    r = requests.get(PI_URL, timeout=30, verify=VERIFY_TLS)
    record("1. reachable + TLS ok", True, f"HTTP {r.status_code} (401 here is expected — auth required)")
except requests.exceptions.SSLError as e:
    record("1. reachable + TLS ok", False, f"TLS/cert error — set VERIFY_TLS to the CA bundle. {str(e)[:150]}")
except Exception as e:
    record("1. reachable + TLS ok", False, f"{type(e).__name__}: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md ## 2. Is Basic advertised? (`WWW-Authenticate` header)
# MAGIC On an **anonymous** request PI lists the schemes it offers unprompted in the
# MAGIC `WWW-Authenticate` header(s). Treat this as a **hint, not a verdict** — it
# MAGIC advertises what's offered, not everything the server will accept.
# MAGIC - `Basic` present → good sign; step 3 confirms.
# MAGIC - only `Negotiate`/`NTLM` → *maybe* Kerberos-first/GPO-disabled — but send Basic
# MAGIC   anyway in step 3 before concluding; the header alone doesn't settle it.

# COMMAND ----------

try:
    r = requests.get(PI_URL, timeout=30, verify=VERIFY_TLS)  # no credentials
    www = r.headers.get("WWW-Authenticate", "")
    print(f"HTTP {r.status_code}")
    print(f"WWW-Authenticate: {www or '(none returned)'}")
    basic_offered = "basic" in www.lower()
    other = [s for s in ("Negotiate", "NTLM", "Bearer") if s.lower() in www.lower()]
    record("2. Basic advertised by PI", basic_offered,
           "Basic is listed" if basic_offered
           else f"Basic not advertised; PI lists: {other or 'nothing'} — a hint only, step 3 is decisive")
except Exception as e:
    record("2. Basic offered by PI", False, f"{type(e).__name__}: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md ## 3. Do the credentials actually work?
# MAGIC A real Basic request. **200 = Basic works end-to-end.** 401 = either bad
# MAGIC credentials/permissions, or Basic is disabled (disambiguate with step 2's header).

# COMMAND ----------

try:
    user = dbutils.secrets.get(SCOPE, USER_KEY)
    pw   = dbutils.secrets.get(SCOPE, PASS_KEY)
    r = requests.get(PI_URL, auth=(user, pw), timeout=30, verify=VERIFY_TLS)
    print(f"HTTP {r.status_code}\n{r.text[:300]}")
    if r.status_code == 200:
        record("3. Basic credentials accepted", True, "HTTP 200 — Basic works end-to-end 🎉")
    elif r.status_code == 401:
        record("3. Basic credentials accepted", False,
               "401 — bad creds/permissions, OR Basic disabled (check step 2: was Basic in WWW-Authenticate?)")
    else:
        record("3. Basic credentials accepted", False, f"unexpected HTTP {r.status_code}")
except Exception as e:
    record("3. Basic credentials accepted", False, f"{type(e).__name__}: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md ## 4. (Optional) does a real PI read work with these creds?
# MAGIC Beyond the root endpoint — resolve one tag, to confirm the account has actual
# MAGIC read permission (auth can succeed but authorization to read points can differ).

# COMMAND ----------

PI_SERVER = "PISRV"                              # EDIT: PI Data Archive server name
TEST_TAG  = "SSE.THERM.KEA.GT1.EXHAUST_TEMP"     # EDIT: any real tag
from urllib.parse import quote
try:
    user = dbutils.secrets.get(SCOPE, USER_KEY); pw = dbutils.secrets.get(SCOPE, PASS_KEY)
    url = f"{PI_URL}/points?path=" + quote(rf"\\{PI_SERVER}\{TEST_TAG}", safe="")
    r = requests.get(url, auth=(user, pw), timeout=30, verify=VERIFY_TLS)
    ok = r.status_code == 200 and "WebId" in r.text
    record("4. can read a point (authorization)", ok,
           f"resolved {TEST_TAG}" if ok else f"HTTP {r.status_code} — auth ok but read denied? check PI permissions")
except Exception as e:
    record("4. can read a point (authorization)", False, f"{type(e).__name__}: {str(e)[:150]}")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

print(f"\n{'='*60}\n  BASIC-AUTH FEASIBILITY PROBE — RESULTS\n{'='*60}")
for step, ok, note in _results:
    print(f"  {'✅' if ok else '❌'}  {step:34s} {note}")
passed = sum(ok for _, ok, _ in _results)
print(f"\n  {passed}/{len(_results)} steps passed")
if passed == len(_results):
    print("  → Basic works AND the account can read. If InfoSec permits Basic, this is\n"
          "    the simplest path: point the connector's endpoint_url at PI and pass\n"
          "    basic_user/basic_password (natively supported — no code change).")
else:
    print("  → Basic did not authenticate from here. Don't conclude 'disabled' from the\n"
          "    header alone — the credential request (step 2) is the real test. If that\n"
          "    truly fails, pursue the in-domain proxy (see AUTH_RUNBOOK.md).")
