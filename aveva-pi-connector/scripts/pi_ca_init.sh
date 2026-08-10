#!/usr/bin/env bash
# Cluster init script — trust PI Web API's internal CA.
#
# WHY: PI presents a certificate issued by the customer's private CA (e.g. "SSE TLS
# Infra Issuing CA1 v3" + its root). Domain-joined machines trust that CA via group
# policy; a Databricks cluster does not, so a verified HTTPS request fails with:
#
#     SSLCertVerificationError: unable to get local issuer certificate
#
# Installing the CA here lets the connector run with verify_tls=true — i.e. TLS fully
# validated, nothing disabled. Runs on EVERY node at startup, so it covers the driver
# and all executors (the connector reads from executors).
#
# IMPORTANT: `requests` (which the connector uses) reads **certifi's** CA bundle, not
# the OS trust store — so appending to /etc/ssl only is NOT enough. This script does
# both: certifi for Python, and the OS store for curl/openssl.
#
# Verified on DBR 16.4 (single-user): appending to certifi's bundle SURVIVES a later
# `%pip install` + `dbutils.library.restartPython()` in the notebook, so no env vars
# are needed (which also avoids the shared-access-mode env-var restrictions).
#
# ── EDIT THIS ─────────────────────────────────────────────────────────────────
CA_PEM="/Volumes/<catalog>/<schema>/<volume>/pi-ca.pem"   # the CA chain (PEM/base64)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

if [[ ! -f "$CA_PEM" ]]; then
  echo "pi_ca_init: ERROR - CA file not found at $CA_PEM" >&2
  exit 1
fi

# 1. Python / requests — the one that actually matters for the connector.
CERTIFI=$(python3 -c "import certifi; print(certifi.where())")
if ! grep -qF "$(head -2 "$CA_PEM" | tail -1)" "$CERTIFI" 2>/dev/null; then
  printf '\n' >> "$CERTIFI"
  cat "$CA_PEM" >> "$CERTIFI"
  echo "pi_ca_init: appended CA to certifi bundle ($CERTIFI)"
else
  echo "pi_ca_init: CA already present in certifi bundle - skipping"
fi

# 2. OS trust store — so curl/openssl//etc also trust it (belt and braces).
cp "$CA_PEM" /usr/local/share/ca-certificates/pi-ca.crt
update-ca-certificates >/dev/null 2>&1 || true
echo "pi_ca_init: installed CA into the OS trust store"

echo "pi_ca_init: done - the connector can now run with verify_tls=true"
