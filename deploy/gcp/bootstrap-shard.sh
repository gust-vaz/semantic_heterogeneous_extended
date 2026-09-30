#!/usr/bin/env bash
# Startup-script for a shard VM: install Docker, then run one shard mongod.
# SHARD_NAME and MONGO_CACHE_GB arrive as instance metadata -> env.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh
SHARD_NAME="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/shard-name)"
MONGO_CACHE_GB="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/mongo-cache-gb)"
mkdir -p /opt/mellow && cd /opt/mellow
curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/compose-shard > compose.shard.yml
SHARD_NAME="$SHARD_NAME" MONGO_CACHE_GB="$MONGO_CACHE_GB" \
  docker compose -f compose.shard.yml up -d
