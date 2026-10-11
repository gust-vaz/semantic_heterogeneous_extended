#!/usr/bin/env bash
# Startup-script for a standalone (single) VM: install Docker, then run one plain
# mongod. MONGO_CACHE_GB arrives as instance metadata -> env; the compose file
# arrives as the compose-node metadata attribute (mirrors bootstrap-member.sh).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh
MONGO_CACHE_GB="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/mongo-cache-gb)"
mkdir -p /opt/mellow && cd /opt/mellow
curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/compose-node > compose.node.yml
MONGO_CACHE_GB="$MONGO_CACHE_GB" \
  docker compose -f compose.node.yml up -d
