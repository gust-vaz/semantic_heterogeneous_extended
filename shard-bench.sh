#!/usr/bin/env bash
# Sharding benchmark entrypoint. Needs only Docker and bash.
#
# Separate from bench.sh on purpose: the S series shares the neutral machinery
# with the B series and nothing else. read_target, read_mode and write_concern
# mean nothing on single-node shards, and chunk size and balancer state mean
# nothing on a replica set.
#
# For each deployment in the sweep: bring the Compose stack up, run the
# experiment inside a runner container attached to that network, then tear the
# stack down with its volumes so the next cell starts clean.
set -euo pipefail

PROFILE="medium"
CORPUS="synthetic"
OUT_DIR="results"
DEPLOYMENTS=""
KEEP_GOING=0
FRESH=""
CHUNK_SIZE_MB=""
CURRENT_COMPOSE=""

usage() {
  cat <<'EOF'
Usage: ./shard-bench.sh <experiment> [options]

Experiments:
  s1    Cost of applying a semantic operation  (default deployments: sh1,sh4,sh7)
  s2    Cost of rebalancing afterwards         (rides along with s1)
  s3    Targeted queries vs broadcast          (default deployments: sh1,sh4,sh7)
  s4    Strategy crossover across scale        (default deployments: sh1,sh4,sh7)
  s5    The price of being schemaless          (default deployments: sh1,sh4,sh7)
  all   Run s1, s3, s4 then s5 (s2 rides along with s1)

Options:
  --profile <smoke|small|medium|full>
                                 Sizing profile (default: medium)
  --deployments <csv>            Override the sweep, e.g. sh4,sh7
  --corpus <synthetic|real>      Data source (default: synthetic)
  --out <dir>                    Output directory (default: results)
  --chunk-size-mb <n>            Cluster-wide chunk size (default: 1)
  --keep-going                   Continue the sweep after a failing cell
  --fresh                        Re-run cells that already have rows
  -h, --help                     Show this help
EOF
}

compose_file_for() {
  case "$1" in
    sh1) echo "docker-compose.shard1.yml" ;;
    sh4) echo "docker-compose.shard4.yml" ;;
    sh7) echo "docker-compose.shard7.yml" ;;
    *)   echo "Unknown deployment '$1'" >&2; return 1 ;;
  esac
}

module_for() {
  case "$1" in
    s1) echo "benchmarks.experiments.s1_operation_cost" ;;
    s2) echo "benchmarks.experiments.s2_rebalance" ;;
    s3) echo "benchmarks.experiments.s3_targeting" ;;
    s4) echo "benchmarks.experiments.s4_crossover_scale" ;;
    s5) echo "benchmarks.experiments.s5_schemaless" ;;
    *)  echo "Unknown experiment '$1'" >&2; return 1 ;;
  esac
}

default_deployments_for() {
  case "$1" in
    s1|s2|s3|s4|s5) echo "sh1,sh4,sh7" ;;
    *)  echo "Unknown experiment '$1'" >&2; return 1 ;;
  esac
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: 'docker' was not found on PATH." >&2
    echo "This harness runs everything in containers; install Docker and retry." >&2
    exit 127
  fi
  if ! docker compose version >/dev/null 2>&1; then
    echo "ERROR: 'docker compose' is unavailable (Compose v2 required)." >&2
    exit 127
  fi
}

teardown() {
  if [ -n "$CURRENT_COMPOSE" ]; then
    echo "  tearing down $CURRENT_COMPOSE"
    docker compose -f "$CURRENT_COMPOSE" down -v >/dev/null 2>&1 || true
    CURRENT_COMPOSE=""
  fi
}
trap 'echo; echo "interrupted"; teardown; exit 130' INT TERM

run_cell() {
  local experiment="$1" deployment="$2"
  local compose module
  compose="$(compose_file_for "$deployment")"
  module="$(module_for "$experiment")"

  echo "[$experiment] deployment=$deployment - starting $compose"
  CURRENT_COMPOSE="$compose"
  docker compose -f "$compose" up -d

  # --user keeps result files owned by the invoking user; without it the
  # container writes root-owned CSVs into the bind-mounted repo that the user
  # then cannot delete.
  local status=0
  docker compose -f "$compose" run --rm \
    --user "$(id -u):$(id -g)" \
    -e BENCH_GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo '')" \
    -e HOME=/tmp \
    runner python -m "$module" \
      --deployment "$deployment" \
      --profile "$PROFILE" \
      --corpus "$CORPUS" \
      --out "$OUT_DIR" \
      ${CHUNK_SIZE_MB:+--chunk-size-mb "$CHUNK_SIZE_MB"} \
      ${FRESH:+$FRESH} || status=$?

  teardown

  if [ "$status" -ne 0 ]; then
    echo "[$experiment] deployment=$deployment FAILED (exit $status)" >&2
    if [ "$KEEP_GOING" -eq 0 ]; then
      exit "$status"
    fi
  else
    echo "[$experiment] deployment=$deployment done"
  fi
  return 0
}

if [ $# -eq 0 ]; then
  usage >&2
  exit 2
fi

EXPERIMENT="$1"
shift

if [ "$EXPERIMENT" = "-h" ] || [ "$EXPERIMENT" = "--help" ]; then
  usage
  exit 0
fi

while [ $# -gt 0 ]; do
  case "$1" in
    --profile)     PROFILE="$2"; shift 2 ;;
    --deployments) DEPLOYMENTS="$2"; shift 2 ;;
    --corpus)      CORPUS="$2"; shift 2 ;;
    --out)         OUT_DIR="$2"; shift 2 ;;
    --keep-going)  KEEP_GOING=1; shift ;;
    --fresh)         FRESH="--fresh"; shift ;;
    --chunk-size-mb) CHUNK_SIZE_MB="$2"; shift 2 ;;
    -h|--help)     usage; exit 0 ;;
    *)             echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done

# Experiments run inside the runner container, where the repo is bind-mounted at
# /app. An absolute host path is therefore written INSIDE the container and
# discarded when it exits, taking both the results and the resume state with it -
# silently, and looking exactly like a campaign that never ran.
case "$OUT_DIR" in
  /*)
    echo "ERROR: --out must be a path inside the repository, not '$OUT_DIR'." >&2
    echo "Experiments run in the runner container with the repo mounted at /app," >&2
    echo "so an absolute host path is written inside the container and lost when" >&2
    echo "it exits - discarding the results and the campaign's resume state." >&2
    echo "Use a repo-relative path, e.g. --out results." >&2
    exit 2
    ;;
esac

if [ "$EXPERIMENT" = "all" ]; then
  EXPERIMENTS="s1 s3 s4 s5"
else
  module_for "$EXPERIMENT" >/dev/null   # validates the name, exits 1 if unknown
  EXPERIMENTS="$EXPERIMENT"
fi

require_docker

for experiment in $EXPERIMENTS; do
  sweep="${DEPLOYMENTS:-$(default_deployments_for "$experiment")}"
  IFS=',' read -ra targets <<< "$sweep"
  for deployment in "${targets[@]}"; do
    run_cell "$experiment" "$deployment"
  done
done

echo "All done. Results in $OUT_DIR/"
