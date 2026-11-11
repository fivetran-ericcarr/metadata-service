#!/bin/bash
# deploy.sh — deploy the metadata-service remote MCP server to Cloud Run.
# Mirrors fivetran-odi-agent-trust-demo/setup/cloudrun/deploy.sh: secrets live
# in Secret Manager, the service is reachable without Cloud Run IAM (external
# agents can't mint Google ID tokens) and authenticates every request itself.
#
#   bash deploy/cloudrun/deploy.sh --init-secrets   # once: create secrets
#   bash deploy/cloudrun/deploy.sh                  # build + deploy
#
# Snapshots: a GCS bucket mounted at /snapshots (Cloud Run volume mount). The
# scheduled `metadata-service build` (CI, cron, Cloud Run job) writes
# latest.json there; the MCP service only reads it (read-only tool surface).
# Nothing in this script runs automatically; review before running.
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-metadata-service-mcp}"
REGION="${REGION:-us-central1}"
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project)}"
SNAPSHOT_BUCKET="${SNAPSHOT_BUCKET:-${PROJECT_ID}-metadata-snapshots}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

_secret() {  # create or add a version; value from stdin
  local name="$1"
  gcloud secrets create "$name" --project="$PROJECT_ID" --data-file=- 2>/dev/null || \
    gcloud secrets versions add "$name" --project="$PROJECT_ID" --data-file=-
}

if [[ "${1:-}" == "--init-secrets" ]]; then
  # Bearer key for Claude / Gemini ADK / Databricks clients. Never rotated by
  # a re-run: agents already hold it.
  if gcloud secrets describe metadata-mcp-api-key --project="$PROJECT_ID" &>/dev/null; then
    echo "metadata-mcp-api-key exists — keeping it"
  else
    openssl rand -hex 32 | tr -d '\n' | _secret metadata-mcp-api-key
    echo "created metadata-mcp-api-key (read it with: gcloud secrets versions access latest --secret metadata-mcp-api-key)"
  fi
  # Optional: Polaris client secret, only needed if this service also BUILDS
  # snapshots (refresh) against a Managed Data Lake destination.
  if [[ -n "${POLARIS_CLIENT_SECRET:-}" ]]; then
    printf '%s' "$POLARIS_CLIENT_SECRET" | _secret metadata-polaris-client-secret
  fi
  gcloud storage buckets describe "gs://$SNAPSHOT_BUCKET" &>/dev/null || \
    gcloud storage buckets create "gs://$SNAPSHOT_BUCKET" --project="$PROJECT_ID" --location="$REGION"
  exit 0
fi

SECRETS="METADATA_API_KEY=metadata-mcp-api-key:latest"
if gcloud secrets describe metadata-polaris-client-secret --project="$PROJECT_ID" &>/dev/null; then
  SECRETS="$SECRETS,POLARIS_CLIENT_SECRET=metadata-polaris-client-secret:latest"
fi

# OAuth (ChatGPT, Snowflake Cortex): export MCP_OAUTH_ISSUER (+ MCP_OAUTH_AUDIENCE)
# before running; MCP_PUBLIC_URL is set after the first deploy (URL known).
ENV_VARS="MCP_TOOLS=${MCP_TOOLS:-read-only},MCP_STATELESS=true,METADATA_LOCAL_PATH=/snapshots"
[[ -n "${SEMANTIC_OVERRIDES_FILE:-}" ]] && ENV_VARS="$ENV_VARS,SEMANTIC_OVERRIDES_FILE=$SEMANTIC_OVERRIDES_FILE"
[[ -n "${MCP_OAUTH_ISSUER:-}" ]] && ENV_VARS="$ENV_VARS,MCP_OAUTH_ISSUER=$MCP_OAUTH_ISSUER"
[[ -n "${MCP_OAUTH_AUDIENCE:-}" ]] && ENV_VARS="$ENV_VARS,MCP_OAUTH_AUDIENCE=$MCP_OAUTH_AUDIENCE"
[[ -n "${MCP_PUBLIC_URL:-}" ]] && ENV_VARS="$ENV_VARS,MCP_PUBLIC_URL=$MCP_PUBLIC_URL"

gcloud run deploy "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --source "$REPO_ROOT" \
  --allow-unauthenticated \
  --port 8080 \
  --memory 1Gi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 3 \
  --update-secrets "$SECRETS" \
  --set-env-vars "$ENV_VARS" \
  --add-volume "name=snapshots,type=cloud-storage,bucket=$SNAPSHOT_BUCKET,readonly=true" \
  --add-volume-mount "volume=snapshots,mount-path=/snapshots"

URL="$(gcloud run services describe "$SERVICE_NAME" --region "$REGION" --project "$PROJECT_ID" \
  --format='value(status.url)')"
echo ""
echo "MCP endpoint: $URL/mcp   (health: $URL/health)"
echo "For OAuth/search citations, redeploy with MCP_PUBLIC_URL=$URL/mcp"
