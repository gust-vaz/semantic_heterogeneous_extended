#!/usr/bin/env bash
# Startup-script for the control VM: Docker, git, clone the repo, build the
# runner image. A shard deployment also starts the config server + router here; a
# replica-set deployment starts nothing locally (its members run on their own
# VMs). KIND arrives as instance metadata.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh
command -v git >/dev/null 2>&1 || (apt-get update && apt-get install -y git)
KIND="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/kind)"
BRANCH="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/repo-branch)"
REPO="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/repo-url)"
mkdir -p /opt/mellow && cd /opt/mellow
[ -d repo ] || git clone --branch "$BRANCH" "$REPO" repo
cd repo
# Build the runner image once; run.sh launches the experiment from this tag.
docker build -t mongo-runner:local docker/runner
if [ "$KIND" = shard ]; then
  CONFIG_HOST="$(hostname -I | awk '{print $1}'):27019"
  CONFIG_HOST="$CONFIG_HOST" docker compose -f deploy/gcp/compose.control.yml up -d
fi
