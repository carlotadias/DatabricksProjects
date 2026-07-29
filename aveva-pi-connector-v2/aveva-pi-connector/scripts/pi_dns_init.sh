#!/usr/bin/env bash
# Cluster init script — pin the PI Web API FQDN to its IP in /etc/hosts.
#
# WHY: the connector uses plain `requests`, which does a normal DNS lookup on the
# hostname in `endpoint_url`. If the cluster's DNS can't resolve the PI FQDN (a
# common gap when the VNet isn't pointed at the internal DNS zone), the connector
# fails with "temporary failure in name resolution" (EAI_AGAIN) before it can
# connect — even though the network route to PI is open.
#
# This adds a static name->IP mapping on every node (driver + executors), so the
# FQDN resolves locally to the IP. Because you still dial the FQDN, the TLS
# certificate (issued for the FQDN) validates — so you can keep verify_tls ON.
#
# This is a STOPGAP for testing. The proper fix is to make the cluster's VNet
# resolve the internal DNS zone (Azure Private DNS / conditional forwarder), after
# which this script is unnecessary. It is also STATIC: if PI's IP changes, update
# the value below. Only valid while the cluster->PI route is actually open (confirm
# with the reachability preflight — connecting to the IP:443 must succeed).
#
# ── EDIT THESE ────────────────────────────────────────────────────────────────
PI_FQDN="<pi-fqdn>"      # e.g. piserver.corp.example.com  (the host in endpoint_url)
PI_IP="<pi-ip>"          # e.g. 10.0.0.5  (from nslookup / your PI admin)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# Append only if not already present (idempotent across restarts).
if ! grep -qE "[[:space:]]${PI_FQDN}(\$|[[:space:]])" /etc/hosts; then
  echo "${PI_IP}  ${PI_FQDN}" >> /etc/hosts
  echo "pi_dns_init: pinned ${PI_FQDN} -> ${PI_IP} in /etc/hosts"
else
  echo "pi_dns_init: ${PI_FQDN} already present in /etc/hosts — leaving as is"
fi
