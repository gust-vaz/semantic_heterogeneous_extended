#!/usr/bin/env bash
# Drive one experiment against a live cloud cluster from the control VM, then
# bring the result CSVs back to the local results-cloud/ directory.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

usage() { echo "Usage: ./run.sh <sh1|sh4|sh7|sh8> [--experiment s1..s5] [--profile smoke] [--dry-run]"; }
DRY_RUN=0; DEPLOYMENT=""; EXPERIMENT="s1"; PROFILE="smoke"
while [ $# -gt 0 ]; do
  case "$1" in
    --experiment) EXPERIMENT="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    sh1|sh4|sh7|sh8) DEPLOYMENT="$1"; shift ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done
[ -n "$DEPLOYMENT" ] || { usage >&2; exit 2; }
run() { if [ "$DRY_RUN" -eq 1 ]; then echo "+ $*"; else "$@"; fi; }

CONTROL="${NAME_PREFIX}-${DEPLOYMENT}-control"
case "$EXPERIMENT" in
  s1) MODULE="benchmarks.experiments.s1_operation_cost" ;;
  s2) MODULE="benchmarks.experiments.s2_rebalance" ;;
  s3) MODULE="benchmarks.experiments.s3_targeting" ;;
  s4) MODULE="benchmarks.experiments.s4_crossover_scale" ;;
  s5) MODULE="benchmarks.experiments.s5_schemaless" ;;
  *) echo "Unknown experiment '$EXPERIMENT'" >&2; exit 2 ;;
esac

# The mongos runs on the control VM's host network; a bounded timeout turns a
# wrong address into a fast failure instead of a hang.
ROUTER="mongodb://localhost:27017/?serverSelectionTimeoutMS=10000"

# Provenance sha computed here: the VM's clone is owned by root (cloned by the
# startup-script), so `git rev-parse` over SSH trips git's dubious-ownership guard
# and returns nothing. The local checkout is the same pushed commit the VM ran.
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo '')"

run gcloud compute ssh "$CONTROL" --zone="$GCP_ZONE" --command="\
  cd /opt/mellow/repo && \
  BENCH_GIT_SHA='${GIT_SHA}' \
  BENCH_ROUTER_URI='${ROUTER}' \
  docker run --rm --network host -e BENCH_ROUTER_URI -e BENCH_GIT_SHA \
    -v \$PWD:/app -w /app mongo-runner:local \
    python -m ${MODULE} --deployment ${DEPLOYMENT} --profile ${PROFILE} --out results"

# Copy results back into a per-deployment directory. Cleared first so a repeated
# `scp --recurse` cannot nest (dest/results/...) or leave another deployment's rows.
DEST="results-cloud/${DEPLOYMENT}"
run rm -rf "$DEST"
run gcloud compute scp --recurse --zone="$GCP_ZONE" \
  "${CONTROL}:/opt/mellow/repo/results" "$DEST"

echo "Ran ${EXPERIMENT} on ${DEPLOYMENT}; results under ${DEST}/"
