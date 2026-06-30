#!/usr/bin/env bash
# Self-contained narrated demo — no credentials required (uses bundled fixtures).
# Great for screen-recording a 2-minute end-to-end walkthrough.
#
#   pip install -e ".[dev,mcp]"
#   ./examples/demo.sh
set -euo pipefail

step() { printf "\n\033[1;36m== %s ==\033[0m\n" "$1"; sleep 1; }

step "1. Build a normalized snapshot from Fivetran + dbt (offline fixtures)"
metadata-service build --fixtures-dir tests/fixtures

step "2. Data Quality recommendations for a table"
metadata-service recommendations --schema salesforce --table account

step "3. Schema drift vs the previous snapshot"
metadata-service drift

step "4. Agent triage flow over MCP (orient -> triage -> drill -> act)"
python examples/agent_quickstart.py || true

echo
echo "Done. In production, swap '--fixtures-dir' for live creds:"
echo "  metadata-service build --group-id <fivetran_group> --dbt-project-id <id>"
