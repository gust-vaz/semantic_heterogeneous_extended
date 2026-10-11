#!/usr/bin/env bash
# Delete one deployment's VMs by label. VPC/firewall are left by default (cheap,
# reused); --all removes them too, even when no VMs remain, so a torn-down
# campaign never strands its network. No matching VM without --all is a
# successful no-op.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

usage() { echo "Usage: ./teardown.sh <sh1|sh4|sh7|sh8|rs3|rs5|single> [--all] [--dry-run]"; }
DRY_RUN=0
DEPLOYMENT=""
ALL=0
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --all) ALL=1; shift ;;
    -h|--help) usage; exit 0 ;;
    sh1|sh4|sh7|sh8|rs3|rs5|single) DEPLOYMENT="$1"; shift ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$DEPLOYMENT" ] || { usage >&2; exit 2; }
run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }

delete_vpc() {
  local net="${NAME_PREFIX}-net"
  run gcloud compute firewall-rules delete "${net}-internal" "${net}-ssh" --quiet || true
  run gcloud compute networks subnets delete "${net}-sub" --region="$GCP_REGION" --quiet || true
  run gcloud compute networks delete "$net" --quiet || true
}

# The list selects the VMs by label; delete then takes their names (delete has no
# --filter). In --dry-run the list is only shown and representative names let the
# delete line be shown.
if [ "$DRY_RUN" -eq 1 ]; then
  echo "+ gcloud compute instances list --filter=labels.deployment=${DEPLOYMENT} --format=name"
  COUNT="$(data_count_for "$DEPLOYMENT")"
  ROLE="$(data_role_for "$DEPLOYMENT")"
  NAMES="${NAME_PREFIX}-${DEPLOYMENT}-control"
  for i in $(seq 1 "$COUNT"); do NAMES="$NAMES ${NAME_PREFIX}-${DEPLOYMENT}-${ROLE}${i}"; done
else
  NAMES="$(gcloud compute instances list \
    --filter="labels.deployment=${DEPLOYMENT}" \
    --format="value(name)" 2>/dev/null || true)"
  if [ -z "$NAMES" ]; then
    echo "Nothing to delete for ${DEPLOYMENT}."
    # --all must still remove the VPC even when no VMs are left. Without --all,
    # this is the no-op it has always been.
    if [ "$ALL" -eq 1 ]; then delete_vpc; echo "Removed the VPC."; fi
    exit 0
  fi
fi
# shellcheck disable=SC2086
run gcloud compute instances delete $NAMES --zone="$GCP_ZONE" --quiet

if [ "$ALL" -eq 1 ]; then delete_vpc; fi
echo "Torn down ${DEPLOYMENT}."
