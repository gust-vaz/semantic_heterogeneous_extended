#!/usr/bin/env bash
# Collect the shard VMs' private IPs, build the env contract, and run the
# parameterized init-sharded.js from the control VM against the live cluster.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

usage() { echo "Usage: ./init-cluster.sh <sh1|sh4|sh7|sh8> [--dry-run]"; }
DRY_RUN=0; DEPLOYMENT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    sh1|sh4|sh7|sh8) DEPLOYMENT="$1"; shift ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$DEPLOYMENT" ] || { usage >&2; exit 2; }
run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }

SHARDS="$(shard_count_for "$DEPLOYMENT")"

# Private IPs of this deployment's shard VMs, ordered by name (shard1..shardN),
# and the control VM's IP. In --dry-run the commands are only shown (their output
# would otherwise be captured, not printed) and fake IPs let the rest proceed.
if [ "$DRY_RUN" -eq 1 ]; then
  echo "+ gcloud compute instances list --filter=labels.deployment=${DEPLOYMENT} AND labels.role=shard --format=networkIP (shard IPs)"
  echo "+ gcloud compute instances list --filter=labels.deployment=${DEPLOYMENT} AND labels.role=control --format=networkIP (control IP)"
  IPS="$(seq -f '10.10.0.%g' 1 "$SHARDS")"
  CONTROL_IP="10.10.0.100"
else
  IPS="$(gcloud compute instances list \
    --filter="labels.deployment=${DEPLOYMENT} AND labels.role=shard" \
    --sort-by=name --format="value(networkInterfaces[0].networkIP)")"
  CONTROL_IP="$(gcloud compute instances list \
    --filter="labels.deployment=${DEPLOYMENT} AND labels.role=control" \
    --format="value(networkInterfaces[0].networkIP)")"
fi
SHARD_HOSTS="$(echo "$IPS" | awk 'NF{printf "%s%s:27018", sep, $0; sep=","}')"
echo "SHARD_HOSTS=${SHARD_HOSTS}"

# Run the init inside a mongo container on the control VM, against its local mongos.
CONTROL="${NAME_PREFIX}-${DEPLOYMENT}-control"
run gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" --command="\
  cd /opt/mellow/repo && \
  docker run --rm --network host \
    -e SHARD_COUNT='${SHARDS}' -e SHARD_HOSTS='${SHARD_HOSTS}' \
    -e CONFIG_HOST='${CONTROL_IP}:27019' -e ROUTER_HOST='localhost:27017' \
    -v \$PWD/docker/sharded/init-sharded.js:/init.js:ro \
    ${MONGO_IMAGE} mongosh --nodb /init.js"
