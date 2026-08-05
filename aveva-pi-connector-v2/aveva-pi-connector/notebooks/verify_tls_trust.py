# Databricks notebook source
# MAGIC %md
# MAGIC # Verify PI's TLS certificate is trusted — driver AND executors
# MAGIC
# MAGIC Run this straight after attaching `scripts/pi_ca_init.sh` and restarting the
# MAGIC cluster. It answers one question: **can we now talk to PI with TLS verification
# MAGIC ON?**
# MAGIC
# MAGIC Why a separate notebook: the connector's own preflight only checks the **driver**,
# MAGIC but the connector reads from **executors** — and the CA has to be present on every
# MAGIC node. A driver-only pass is not conclusive.
# MAGIC
# MAGIC No credentials needed. This is a pure TLS handshake — it never authenticates, so
# MAGIC it isolates "is the certificate trusted?" from "do my credentials work?".

# COMMAND ----------

ENDPOINT_URL = "https://<host>/piwebapi"   # EDIT — PI Web API base (same as the other notebooks)
N_PROBES     = 8                           # spread across executors; raise for a bigger cluster

# Host/port are derived from ENDPOINT_URL, so this is the only line you edit.
from urllib.parse import urlparse
PI_FQDN = urlparse(ENDPOINT_URL).hostname
PI_PORT = urlparse(ENDPOINT_URL).port or 443
print(f"testing TLS trust for {PI_FQDN}:{PI_PORT}")

# COMMAND ----------

# MAGIC %md ## 1. Driver — is the CA present, and does the handshake verify?

# COMMAND ----------

import ssl, socket, certifi, os

print(f"certifi bundle : {certifi.where()}")
print(f"bundle size    : {os.path.getsize(certifi.where()):,} bytes")
print(f"OS trust store : /etc/ssl/certs/ca-certificates.crt "
      f"({os.path.getsize('/etc/ssl/certs/ca-certificates.crt'):,} bytes)")
print(f"REQUESTS_CA_BUNDLE = {os.environ.get('REQUESTS_CA_BUNDLE', '(unset — fine, certifi is the default)')}")

try:
    ctx = ssl.create_default_context()          # verification ON
    with socket.create_connection((PI_FQDN, PI_PORT), timeout=10) as raw:
        with ctx.wrap_socket(raw, server_hostname=PI_FQDN) as tls:
            cert = tls.getpeercert()
    subj = dict(x[0] for x in cert["subject"]).get("commonName", "?")
    issr = dict(x[0] for x in cert["issuer"]).get("commonName", "?")
    print(f"\n✅ DRIVER: certificate VERIFIED")
    print(f"   subject : {subj}")
    print(f"   issuer  : {issr}          <- the CA we installed")
    print(f"   expires : {cert.get('notAfter')}")
    print(f"   SANs    : {[v for k, v in cert.get('subjectAltName', ()) if k == 'DNS'][:5]}")
except ssl.SSLCertVerificationError as e:
    print(f"\n❌ DRIVER: NOT trusted — {e.verify_message or e}")
    print("   The CA isn't in certifi's bundle on this node. Check the init script ran:")
    print("   Compute -> Event log, and Driver logs for 'pi_ca_init:' lines.")
except Exception as e:
    print(f"\nℹ️  DRIVER: inconclusive ({e!r}) — not a cert-trust error. "
          f"Check DNS/network before reading anything into this.")

# COMMAND ----------

# MAGIC %md ## 2. Executors — the one that actually matters
# MAGIC The connector's reads happen here. Uses `mapInPandas` so it works in shared
# MAGIC access mode too (no RDD / sparkContext).

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

_CFG = {"host": PI_FQDN, "port": PI_PORT}
_SCHEMA = StructType([
    StructField("executor", StringType(), False),
    StructField("result", StringType(), False),
    StructField("detail", StringType(), True),
])

def _probe(iterator):
    import ssl, socket, pandas as pd
    host, port = _CFG["host"], _CFG["port"]
    for pdf in iterator:
        rows = []
        for _ in range(len(pdf)):
            try:
                ctx = ssl.create_default_context()      # verification ON
                with socket.create_connection((host, port), timeout=10) as raw:
                    with ctx.wrap_socket(raw, server_hostname=host) as tls:
                        issuer = dict(x[0] for x in tls.getpeercert()["issuer"]).get("commonName", "?")
                rows.append((socket.gethostname(), "TRUSTED", f"issuer={issuer}"))
            except ssl.SSLCertVerificationError as e:
                rows.append((socket.gethostname(), "NOT_TRUSTED", str(e.verify_message or e)[:120]))
            except Exception as e:
                rows.append((socket.gethostname(), "INCONCLUSIVE", f"{type(e).__name__}: {str(e)[:100]}"))
        yield pd.DataFrame(rows, columns=["executor", "result", "detail"])

probes = (spark.range(N_PROBES).repartition(N_PROBES)
          .mapInPandas(_probe, schema=_SCHEMA).collect())

by_host = {}
for r in probes:
    by_host.setdefault(r["executor"], (r["result"], r["detail"]))

print(f"probed {len(probes)} task(s) across {len(by_host)} distinct executor host(s)\n")
for host, (res, detail) in by_host.items():
    icon = {"TRUSTED": "✅", "NOT_TRUSTED": "❌"}.get(res, "ℹ️")
    print(f"  {icon} {host}: {res}  {detail}")

# COMMAND ----------

# MAGIC %md ## Verdict

# COMMAND ----------

exec_ok = bool(by_host) and all(v[0] == "TRUSTED" for v in by_host.values())
print("=" * 66)
if exec_ok:
    print("  ✅ TLS VERIFIED ON ALL EXECUTORS")
    print("  → Run the connector with verify_tls=true (i.e. VERIFY_TLS = True in the")
    print("    test notebooks). Nothing disabled; the certificate is properly validated.")
else:
    print("  ❌ NOT all executors trust the certificate")
    print("  → The init script likely didn't run on every node. Check:")
    print("     - it is attached under Compute -> Advanced -> Init scripts")
    print("     - the cluster was RESTARTED after attaching it")
    print("     - the CA_PEM path in the script is readable from the cluster")
    print("     - Driver/executor logs for 'pi_ca_init:' output")
    print("  → Until fixed, keep verify_tls=false as a diagnostic only.")
print("=" * 66)
