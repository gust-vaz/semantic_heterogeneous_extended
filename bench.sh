#!/usr/bin/env bash
# Benchmark entrypoint. Needs only Docker and bash.
#
# For each deployment in the sweep: bring the Compose stack up, run the
# experiment inside a runner container attached to that network, then tear the
# stack down with its volumes so the next cell starts clean.
set -euo pipefail

PROFILE="small"
CORPUS="synthetic"
OUT_DIR="results"
DEPLOYMENTS=""
KEEP_GOING=0
CURRENT_COMPOSE=""

usage() {
  cat <<'EOF'
Usage: ./bench.sh <experiment> [options]

Experiments:
  b1    Read offloading throughput      (default deployments: single,rs3,rs5)
  b3    Write cost of replication       (default deployments: single,rs3,rs5)
  b4    Strategy x distribution         (default deployments: single,rs3,rs5)
  b6    Version-chain depth             (default deployments: single,rs3)
  all   Run b1, b3, b6 then b4

Options:
  --profile <smoke|small|full>   Sizing profile (default: small)
  --deployments <csv>            Override the sweep, e.g. single,rs3
  --corpus <synthetic|real>      Data source (default: synthetic)
  --out <dir>                    Output directory (default: results)
  --keep-going                   Continue the sweep after a failing cell
  -h, --help                     Show this help
EOF
}

compose_file_for() {
  case "$1" in
    single) echo "docker-compose.yml" ;;
    rs3)    echo "docker-compose.replicaset.yml" ;;
    rs5)    echo "docker-compose.replicaset5.yml" ;;
    *)      echo "Unknown deployment '$1'" >&2; return 1 ;;
  esac
}

module_for() {
  case "$1" in
    b1) echo "benchmarks.experiments.b1_read_offloading" ;;
    b3) echo "benchmarks.experiments.b3_write_replication" ;;
    b4) echo "benchmarks.experiments.b4_strategy_distribution" ;;
    b6) echo "benchmarks.experiments.b6_chain_depth" ;;
    *)  echo "Unknown experiment '$1'" >&2; return 1 ;;
  esac
}

default_deployments_for() {
  case "$1" in
    b1) echo "single,rs3,rs5" ;;
    b3) echo "single,rs3,rs5" ;;
    b4) echo "single,rs3,rs5" ;;
    b6) echo "single,rs3" ;;
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
      --out "$OUT_DIR" || status=$?

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
    -h|--help)     usage; exit 0 ;;
    *)             echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done

if [ "$EXPERIMENT" = "all" ]; then
  EXPERIMENTS="b1 b3 b6 b4"
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
