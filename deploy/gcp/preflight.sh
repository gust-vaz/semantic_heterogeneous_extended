#!/usr/bin/env bash
# Verify the GCP account is ready before any VM is created. Prints PASS/FAIL for
# each check with the exact remedy, and exits nonzero if any check fails.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$HERE/config.env"

usage() { echo "Usage: ./preflight.sh [--deployment sh1|sh4|sh8]"; }
DEPLOYMENT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --deployment) DEPLOYMENT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option '$1'" >&2; usage >&2; exit 2 ;;
  esac
done

fails=0
pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; echo "      -> $2"; fails=$((fails + 1)); }

if command -v gcloud >/dev/null 2>&1; then
  pass "gcloud installed"
else
  fail "gcloud installed" "Install the Google Cloud SDK, then re-run."
  echo "$fails check(s) failed."; exit 1
fi

if gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null | grep -q .; then
  pass "authenticated"
else
  fail "authenticated" "Run: gcloud auth login"
fi

PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
if [ -n "$PROJECT" ] && [ "$PROJECT" != "(unset)" ]; then
  pass "project set ($PROJECT)"
else
  fail "project set" "Run: gcloud config set project <PROJECT_ID>"
fi

if [ -n "$PROJECT" ] && [ "$PROJECT" != "(unset)" ]; then
  if gcloud beta billing projects describe "$PROJECT" \
       --format="value(billingEnabled)" 2>/dev/null | grep -qi true; then
    pass "billing enabled"
  else
    fail "billing enabled" "Link the trial billing account to $PROJECT in the console."
  fi

  if gcloud services list --enabled --filter="config.name:compute.googleapis.com" \
       --format="value(config.name)" 2>/dev/null | grep -q compute; then
    pass "compute API enabled"
  else
    fail "compute API enabled" "Run: gcloud services enable compute.googleapis.com"
  fi
fi

if [ -n "$DEPLOYMENT" ]; then
  shards="$(shard_count_for "$DEPLOYMENT")"
  needed=$((4 + shards * 2))   # n2-standard-4 control + e2-small shards (2 vCPU each)
  quota="$(gcloud compute regions describe "$GCP_REGION" \
             --format="value(quotas.filter(\"metric=CPUS\").limit.firstof())" 2>/dev/null || echo 0)"
  quota="${quota%.*}"
  if [ -n "$quota" ] && [ "$quota" -ge "$needed" ] 2>/dev/null; then
    pass "vCPU quota ($quota >= $needed for $DEPLOYMENT)"
  else
    fail "vCPU quota" "Need $needed CPUS in $GCP_REGION (have ${quota:-0}); request an increase."
  fi
fi

echo
if [ "$fails" -eq 0 ]; then echo "All checks passed."; exit 0; fi
echo "$fails check(s) failed."; exit 1
