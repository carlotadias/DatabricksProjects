#!/usr/bin/env bash
# Build BOTH wheels (connector + library), bundle deploy (provisions the UC
# Volume), and publish both wheels to that Volume.
#
#   ./scripts/deploy.sh <dev|prod>
#
# ─── CONFIG — THE ONE PLACE TO EDIT ─────────────────────────────────────────
# These three values are the single source of truth for WHERE the wheels go.
# They flow into BOTH the bundle deploy (via BUNDLE_VAR_*, which databricks.yml
# reads through ${var.*}) AND the wheel-publish path below — so the two can't drift.
CATALOG="main"                                          # UC catalog for the shared lib volume
LIBS_SCHEMA="ops"                                       # UC schema
LIBS_VOLUME="common_libs"                               # UC volume the wheels are published to
# WHICH workspace is an auth concern, NOT set here: pass a CLI profile
#   (PROFILE=<name> ./scripts/deploy.sh dev) or export DATABRICKS_HOST/TOKEN.
# ────────────────────────────────────────────────────────────────────────────
set -euo pipefail

TARGET="${1:-dev}"
PROFILE_ARG="${PROFILE:+-p $PROFILE}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

# Feed the single config to the bundle. databricks.yml resolves ${var.*} from these.
export BUNDLE_VAR_catalog="$CATALOG"
export BUNDLE_VAR_libs_schema="$LIBS_SCHEMA"
export BUNDLE_VAR_libs_volume="$LIBS_VOLUME"
VOLUME_PATH="/Volumes/${CATALOG}/${LIBS_SCHEMA}/${LIBS_VOLUME}"

echo "==> 1/3 build both wheels (connector + library)"
for pkg in timeseries assetframework; do
  (cd "$HERE/$pkg" && python -m build --wheel --outdir dist)
done

echo "==> 2/3 bundle deploy (target=$TARGET)"
(cd "$HERE" && databricks bundle deploy -t "$TARGET" $PROFILE_ARG)

echo "==> 3/3 publish both wheels to $VOLUME_PATH"
for pkg in timeseries assetframework; do
  WHEEL=$(ls -t "$HERE/$pkg"/dist/*.whl | head -1)
  databricks fs cp "$WHEEL" "dbfs:${VOLUME_PATH}/$(basename "$WHEEL")" --overwrite $PROFILE_ARG
  echo "    published $(basename "$WHEEL")"
done
echo "==> done. Install on clusters via:"
echo "    pip install ${VOLUME_PATH}/aveva_pi_assetframework-3.0.1-py3-none-any.whl"
echo "    pip install ${VOLUME_PATH}/aveva_pi_timeseries-2.0.3-py3-none-any.whl"
