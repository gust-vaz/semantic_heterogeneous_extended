#!/usr/bin/env bash
# Collect the member VMs' private IPs, build REPLSET_HOSTS (member1 first so it
# leads), and run the parameterized init-replicaset-cloud.js from the control VM.
# The mongosh container reaches the members by private IP. --dry-run prints the
# commands and uses placeholder IPs.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

usage() { echo "Usage: ./init-replicaset.sh <rs3|rs5> [--dry-run]"; }
DRY_RUN=0
DEPLOYMENT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    rs3|rs5) DEPLOYMENT="$1"; shift ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$DEPLOYMENT" ] || { usage >&2; exit 2; }
run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }

MEMBERS="$(data_count_for "$DEPLOYMENT")"

# Member VMs' private IPs, ordered by name (member1..memberN). In --dry-run the
# list command is only shown and placeholder IPs let the rest proceed.
if [ "$DRY_RUN" -eq 1 ]; then
  echo "+ gcloud compute instances list --filter=labels.deployment=${DEPLOYMENT} AND labels.role=member --format=networkIP (member IPs)"
  IPS="$(seq -f '10.10.0.%g' 1 "$MEMBERS")"
else
  IPS="$(gcloud compute instances list \
    --filter="labels.deployment=${DEPLOYMENT} AND labels.role=member" \
    --sort-by=name --format="value(networkInterfaces[0].networkIP)")"
fi
REPLSET_HOSTS="$(echo "$IPS" | awk 'NF{printf "%s%s:27017", sep, $0; sep=","}')"
echo "REPLSET_HOSTS=${REPLSET_HOSTS}"

# Run the init inside a mongo container on the control VM, reaching members by IP.
CONTROL="${NAME_PREFIX}-${DEPLOYMENT}-control"
run gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" --command="\
  cd /opt/mellow/repo && \
  docker run --rm --network host \
    -e REPLSET_HOSTS='${REPLSET_HOSTS}' \
    -v \$PWD/docker/replicaset/init-replicaset-cloud.js:/init.js:ro \
    ${MONGO_IMAGE} mongosh --nodb /init.js"
