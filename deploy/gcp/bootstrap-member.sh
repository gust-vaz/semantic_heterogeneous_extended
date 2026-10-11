#!/usr/bin/env bash
# Startup-script for a replica-set member VM: install Docker, then run one mongod.
# MONGO_CACHE_GB arrives as instance metadata -> env; the compose file arrives as
# the compose-member metadata attribute (mirrors bootstrap-shard.sh).
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh
MONGO_CACHE_GB="$(curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/mongo-cache-gb)"
mkdir -p /opt/mellow && cd /opt/mellow
curl -s -H 'Metadata-Flavor: Google' \
  http://metadata/computeMetadata/v1/instance/attributes/compose-member > compose.member.yml
MONGO_CACHE_GB="$MONGO_CACHE_GB" \
  docker compose -f compose.member.yml up -d
