#!/usr/bin/env bash
# Capgemini DQaaS quickstart — REST API.
# The same metadata is available over HTTP for agents/services that prefer REST.
#
#   metadata-service serve-api          # starts on :8080
#   ./examples/rest_quickstart.sh
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"

echo "== health =="
curl -s "$BASE/health"; echo

echo "== orient: full snapshot is large; pull the sections you need =="
# Warehouse objects, filtered (compact):
curl -s "$BASE/metadata/warehouse-objects?schema=github&table=issue" | head -c 800; echo

echo "== recommendations for one object =="
curl -s "$BASE/dq/recommendations?schema=github&table=issue"; echo

echo "== drift since previous snapshot (high severity only) =="
curl -s "$BASE/dq/drift?severity=high"; echo

echo "== trigger a refresh (re-extract + new snapshot) =="
curl -s -X POST "$BASE/metadata/refresh" \
  -H 'content-type: application/json' \
  -d '{"include_fivetran": true, "include_dbt": true}'; echo
