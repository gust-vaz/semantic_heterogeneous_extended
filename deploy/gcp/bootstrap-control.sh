#!/usr/bin/env bash
# Startup-script for the control VM: Docker, clone the repo, run cfg + mongos.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh
command -v git >/dev/null 2>&1 || (apt-get update && apt-get install -y git)
CONFIG_HOST="$(hostname -I | awk '{print $1}'):27019"
BRANCH="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/repo-branch)"
REPO="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/repo-url)"
mkdir -p /opt/mellow && cd /opt/mellow
[ -d repo ] || git clone --branch "$BRANCH" "$REPO" repo
cd repo
# Build the runner image once; run.sh launches the experiment from this tag.
docker build -t mongo-runner:local docker/runner
CONFIG_HOST="$CONFIG_HOST" docker compose -f deploy/gcp/compose.control.yml up -d
