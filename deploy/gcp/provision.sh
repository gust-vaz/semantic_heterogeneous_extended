#!/usr/bin/env bash
# Provision one deployment's cluster on GCP: a VPC/subnet/firewall (idempotent)
# and one control VM + N shard VMs. --dry-run prints the gcloud commands.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

usage() { echo "Usage: ./provision.sh <sh1|sh4|sh8> [--dry-run]"; }
DRY_RUN=0
DEPLOYMENT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    sh1|sh4|sh8) DEPLOYMENT="$1"; shift ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$DEPLOYMENT" ] || { echo "Deployment required" >&2; usage >&2; exit 2; }
SHARDS="$(shard_count_for "$DEPLOYMENT")" || { echo "Unknown deployment '$DEPLOYMENT'" >&2; exit 2; }

run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }

# Quota guard (skipped in dry-run, where no gcloud values are real).
if [ "$DRY_RUN" -eq 0 ]; then
  needed=$((4 + SHARDS * 2))
  quota="$(gcloud compute regions describe "$GCP_REGION" \
            --format="value(quotas.filter(\"metric=CPUS\").limit.firstof())" 2>/dev/null || echo 0)"
  quota="${quota%.*}"
  if ! { [ -n "$quota" ] && [ "$quota" -ge "$needed" ] 2>/dev/null; }; then
    echo "ERROR: vCPU quota in $GCP_REGION is ${quota:-0}, need $needed for $DEPLOYMENT." >&2
    echo "Request a quota increase or provision a smaller deployment first." >&2
    exit 1
  fi
fi

NET="${NAME_PREFIX}-net"
LABELS="campaign=mellow,deployment=${DEPLOYMENT}"

# SSH is opened only to the operator's IP. Set AUTHOR_IP to skip the lookup;
# never fall back to 0.0.0.0/0, and never hit the network during --dry-run.
if [ -n "${AUTHOR_IP:-}" ]; then SSH_SRC="${AUTHOR_IP}/32"
elif [ "$DRY_RUN" -eq 1 ]; then SSH_SRC="YOUR_IP/32"
else SSH_SRC="$(curl -s ifconfig.me)/32"; fi

# VPC + subnet + firewall, idempotent (|| true so a re-run is harmless).
run gcloud compute networks create "$NET" --subnet-mode=custom || true
run gcloud compute networks subnets create "${NET}-sub" \
  --network="$NET" --region="$GCP_REGION" --range="$GCP_SUBNET_CIDR" || true
run gcloud compute firewall-rules create "${NET}-internal" \
  --network="$NET" --allow=tcp:27017-27019 --source-ranges="$GCP_SUBNET_CIDR" || true
run gcloud compute firewall-rules create "${NET}-ssh" \
  --network="$NET" --allow=tcp:22 --source-ranges="$SSH_SRC" || true

# Control VM.
run gcloud compute instances create "${NAME_PREFIX}-${DEPLOYMENT}-control" \
  --zone="$GCP_ZONE" --machine-type="$CONTROL_MACHINE" \
  --subnet="${NET}-sub" --labels="${LABELS},role=control" \
  --metadata-from-file=startup-script="$HERE/bootstrap-control.sh" \
  --metadata=repo-branch=cloud-addition,repo-url="${REPO_URL}"

# Shard VMs.
for i in $(seq 1 "$SHARDS"); do
  run gcloud compute instances create "${NAME_PREFIX}-${DEPLOYMENT}-shard${i}" \
    --zone="$GCP_ZONE" --machine-type="$SHARD_MACHINE" \
    --subnet="${NET}-sub" --labels="${LABELS},role=shard" \
    --metadata-from-file=startup-script="$HERE/bootstrap-shard.sh",compose-shard="$HERE/compose.shard.yml" \
    --metadata=shard-name="shard${i}",mongo-cache-gb=0.5
done

echo "Provisioned $DEPLOYMENT: $SHARDS shard VM(s) + 1 control."
