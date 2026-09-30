#!/usr/bin/env bash
# Start one deployment's VMs again after a stop.sh between sessions.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"
usage() { echo "Usage: ./start.sh <sh1|sh4|sh8> [--dry-run]"; }
DRY_RUN=0; DEPLOYMENT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    sh1|sh4|sh8) DEPLOYMENT="$1"; shift ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$DEPLOYMENT" ] || { usage >&2; exit 2; }
run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }
NAMES="$(gcloud compute instances list --filter="labels.deployment=${DEPLOYMENT}" \
  --format="value(name)" 2>/dev/null || true)"
# shellcheck disable=SC2086
run gcloud compute instances start $NAMES --zone="$GCP_ZONE"
echo "Started ${DEPLOYMENT}."
