#!/usr/bin/env bash
# List every campaign VM and its state, so nothing is left billing unnoticed.
set -euo pipefail
usage() { echo "Usage: ./status.sh [--dry-run]"; }
DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }
run gcloud compute instances list --filter="labels.campaign=mellow" \
  --format="table(name, labels.deployment, status, machineType.basename())"
