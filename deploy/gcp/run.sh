#!/usr/bin/env bash
# Drive one experiment against a live cloud cluster from the control VM, then
# bring the result CSVs back to the local results-cloud/ directory. Shard
# deployments (sh*) run an S-series module against the control VM's local router;
# replica-set deployments (rs*) run a B-series module against the member VMs, with
# the primary and secondaries passed through the harness's URI overrides.
# --corpus real first uploads the git-ignored DATASUS dataset to the control VM.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

usage() { echo "Usage: ./run.sh <sh1|sh4|sh7|sh8|rs3|rs5|single> [--experiment s1..s5|b1..b4] [--profile smoke] [--corpus synthetic|real] [--dry-run]"; }
DRY_RUN=0
DEPLOYMENT=""
EXPERIMENT=""
PROFILE="smoke"
CORPUS="synthetic"
while [ $# -gt 0 ]; do
  case "$1" in
    --experiment) EXPERIMENT="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --corpus) CORPUS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    sh1|sh4|sh7|sh8|rs3|rs5|single) DEPLOYMENT="$1"; shift ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$DEPLOYMENT" ] || { usage >&2; exit 2; }
run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }
KIND="$(kind_for "$DEPLOYMENT")"
CONTROL="${NAME_PREFIX}-${DEPLOYMENT}-control"

# Default experiment by kind: the S-series anchor for shards, B-series for sets.
if [ -z "$EXPERIMENT" ]; then
  if [ "$KIND" = shard ]; then EXPERIMENT="s1"; else EXPERIMENT="b1"; fi
fi
case "$EXPERIMENT" in
  s1) MODULE="benchmarks.experiments.s1_operation_cost" ;;
  s2) MODULE="benchmarks.experiments.s2_rebalance" ;;
  s3) MODULE="benchmarks.experiments.s3_targeting" ;;
  s4) MODULE="benchmarks.experiments.s4_crossover_scale" ;;
  s5) MODULE="benchmarks.experiments.s5_schemaless" ;;
  b1) MODULE="benchmarks.experiments.b1_read_offloading" ;;
  b2) MODULE="benchmarks.experiments.b2_write_replication" ;;
  b3) MODULE="benchmarks.experiments.b3_strategy_distribution" ;;
  b4) MODULE="benchmarks.experiments.b4_chain_depth" ;;
  *) echo "Unknown experiment '$EXPERIMENT'" >&2; exit 2 ;;
esac
case "$CORPUS" in
  synthetic|real) ;;
  *) echo "Unknown corpus '$CORPUS' (synthetic|real)" >&2; exit 2 ;;
esac
# B4 sweeps the chain length it builds; the real corpus brings its own fixed
# operations, so every cell would measure the same corpus. Refused before any
# upload is paid for.
if [ "$EXPERIMENT" = b4 ] && [ "$CORPUS" = real ]; then
  echo "b4 sweeps synthetic chain lengths; it cannot run on --corpus real" >&2; exit 2
fi

# Provenance sha computed here: the VM's clone is owned by root (cloned by the
# startup-script), so `git rev-parse` over SSH trips git's dubious-ownership guard
# and returns nothing. The local checkout is the same pushed commit the VM ran.
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo '')"

# The DATASUS dataset is git-ignored, so the VM's clone does not have it. It is
# shipped once as a tarball (source CSVs + operations, ~50 MB compressed) into the
# path the harness reads, relative to the repo; a VM that already has the
# operations file is left alone. The clone is root-owned, hence sudo.
if [ "$CORPUS" = real ]; then
  DATASET="dataset/MellowDB_experiments"
  if [ ! -d "$DATASET/source_data" ] || [ ! -d "$DATASET/semantic_operations" ]; then
    echo "DATASUS dataset not found at '$DATASET' (source_data/ + semantic_operations/)" >&2; exit 2
  fi
  REMOTE="/opt/mellow/repo/$DATASET"
  if [ "$DRY_RUN" -eq 1 ] || ! gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" \
      --command="test -f $REMOTE/semantic_operations/operations_cid9_cid10.csv"; then
    TARBALL="$(mktemp --suffix=.tgz)"
    run tar czf "$TARBALL" -C "$DATASET" source_data semantic_operations
    run gcloud compute scp --zone="$GCP_ZONE" "$TARBALL" "${CONTROL}:/tmp/mellow-dataset.tgz"
    run gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" --command="\
      sudo mkdir -p $REMOTE && sudo tar xzf /tmp/mellow-dataset.tgz -C $REMOTE && rm /tmp/mellow-dataset.tgz"
    rm -f "$TARBALL"
  fi
fi

if [ "$KIND" = shard ]; then
  # The mongos runs on the control VM's host network; a bounded timeout turns a
  # wrong address into a fast failure instead of a hang.
  ROUTER="mongodb://localhost:27017/?serverSelectionTimeoutMS=10000"
  run gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" --command="\
    cd /opt/mellow/repo && \
    BENCH_GIT_SHA='${GIT_SHA}' \
    BENCH_ROUTER_URI='${ROUTER}' \
    docker run --rm --network host -e BENCH_ROUTER_URI -e BENCH_GIT_SHA \
      -v \$PWD:/app -w /app mongo-runner:local \
      python -m ${MODULE} --deployment ${DEPLOYMENT} --profile ${PROFILE} --corpus ${CORPUS} --out results"
elif [ "$KIND" = replicaset ]; then
  # Replica-set members run on their own VMs. The write URI names the whole set
  # (so writes follow an election); the read URIs are pinned to the secondaries,
  # members 2..N (member1 leads by priority). A bounded selection timeout fails
  # fast on a wrong address. In --dry-run the member list is only shown and
  # placeholder IPs let the override strings be shown.
  MEMBERS="$(data_count_for "$DEPLOYMENT")"
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "+ gcloud compute instances list --filter=labels.deployment=${DEPLOYMENT} AND labels.role=member --format=networkIP (member IPs)"
    IPS="$(seq -f '10.10.0.%g' 1 "$MEMBERS")"
  else
    IPS="$(gcloud compute instances list \
      --filter="labels.deployment=${DEPLOYMENT} AND labels.role=member" \
      --sort-by=name --format="value(networkInterfaces[0].networkIP)")"
  fi
  SEEDS="$(echo "$IPS" | awk 'NF{printf "%s%s:27017", sep, $0; sep=","}')"
  PRIMARY_URI="mongodb://${SEEDS}/?replicaSet=rs0&serverSelectionTimeoutMS=10000"
  SECONDARY_URIS="$(echo "$IPS" | awk 'NR>1{printf "%smongodb://%s:27017/?directConnection=true", sep, $0; sep=","}')"
  run gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" --command="\
    cd /opt/mellow/repo && \
    BENCH_GIT_SHA='${GIT_SHA}' \
    BENCH_PRIMARY_URI='${PRIMARY_URI}' \
    BENCH_SECONDARY_URIS='${SECONDARY_URIS}' \
    docker run --rm --network host -e BENCH_PRIMARY_URI -e BENCH_SECONDARY_URIS -e BENCH_GIT_SHA \
      -v \$PWD:/app -w /app mongo-runner:local \
      python -m ${MODULE} --deployment ${DEPLOYMENT} --profile ${PROFILE} --corpus ${CORPUS} --out results"
else
  # A single standalone node on its own VM: the 1-node B-series baseline. The
  # write URI is a direct connection to that node; there is no secondary to
  # offload reads to, so BENCH_SECONDARY_URIS is left unset. A bounded selection
  # timeout fails fast on a wrong address.
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "+ gcloud compute instances list --filter=labels.deployment=${DEPLOYMENT} AND labels.role=node --format=networkIP (node IP)"
    NODE_IP="10.10.0.1"
  else
    NODE_IP="$(gcloud compute instances list \
      --filter="labels.deployment=${DEPLOYMENT} AND labels.role=node" \
      --format="value(networkInterfaces[0].networkIP)")"
  fi
  PRIMARY_URI="mongodb://${NODE_IP}:27017/?directConnection=true&serverSelectionTimeoutMS=10000"
  run gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" --command="\
    cd /opt/mellow/repo && \
    BENCH_GIT_SHA='${GIT_SHA}' \
    BENCH_PRIMARY_URI='${PRIMARY_URI}' \
    docker run --rm --network host -e BENCH_PRIMARY_URI -e BENCH_GIT_SHA \
      -v \$PWD:/app -w /app mongo-runner:local \
      python -m ${MODULE} --deployment ${DEPLOYMENT} --profile ${PROFILE} --corpus ${CORPUS} --out results"
fi

# Copy results back into a per-deployment directory. Cleared first so a repeated
# `scp --recurse` cannot nest (dest/results/...) or leave another deployment's rows.
DEST="results-cloud/${DEPLOYMENT}"
run rm -rf "$DEST"
run gcloud compute scp --recurse --zone="$GCP_ZONE" \
  "${CONTROL}:/opt/mellow/repo/results" "$DEST"

echo "Ran ${EXPERIMENT} on ${DEPLOYMENT}; results under ${DEST}/"
