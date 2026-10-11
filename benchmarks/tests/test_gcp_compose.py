"""The cloud per-role Compose files, resolved by Compose itself (no bring-up).

compose.shard.yml is one shard mongod per VM; compose.control.yml is the config
server plus the router. Both require an address variable (SHARD_NAME / CONFIG_HOST)
so a misconfigured VM fails loudly rather than starting wrong, so the tests supply
them the way the bootstrap scripts do.
"""
import pytest

from benchmarks.tests.compose_config import command, needs_docker, resolved

CONTROL = "deploy/gcp/compose.control.yml"
SHARD = "deploy/gcp/compose.shard.yml"
MEMBER = "deploy/gcp/compose.member.yml"
NODE = "deploy/gcp/compose.node.yml"


@needs_docker
def test_shard_stack_is_a_single_shardsvr():
    cmds = [command(s) for s in resolved(SHARD, SHARD_NAME="shard1").values()]
    assert sum("--shardsvr" in c for c in cmds) == 1
    assert sum("--configsvr" in c for c in cmds) == 0
    assert all("--replSet" in c for c in cmds if "--shardsvr" in c)


@needs_docker
def test_control_stack_is_configsvr_plus_router():
    cmds = [command(s) for s in resolved(CONTROL, CONFIG_HOST="10.0.0.1:27019").values()]
    assert sum("--configsvr" in c for c in cmds) == 1
    assert sum(c.startswith("mongos ") for c in cmds) == 1


@needs_docker
@pytest.mark.parametrize("compose", [CONTROL, SHARD])
def test_every_mongo_raises_the_open_file_limit(compose):
    services = resolved(compose, SHARD_NAME="shard1", CONFIG_HOST="10.0.0.1:27019")
    servers = {n: s for n, s in services.items()
               if command(s).startswith(("mongod ", "mongos "))}
    assert servers
    for name, service in servers.items():
        assert service.get("ulimits", {}).get("nofile") == {"soft": 64000, "hard": 64000}, name


@needs_docker
def test_shard_cache_is_raisable_without_editing():
    for env_value, expected in ((None, "0.25"), ("1.5", "1.5")):
        cmds = [command(s) for s in
                resolved(SHARD, SHARD_NAME="shard1", MONGO_CACHE_GB=env_value).values()
                if command(s).startswith("mongod ")]
        assert cmds
        assert all(f"--wiredTigerCacheSizeGB {expected}" in c for c in cmds)


@needs_docker
def test_member_stack_is_a_single_replset_mongod():
    cmds = [command(s) for s in resolved(MEMBER).values()]
    assert len(cmds) == 1
    assert "--replSet rs0" in cmds[0]
    assert "--port 27017" in cmds[0]
    assert "--shardsvr" not in cmds[0]
    assert "--configsvr" not in cmds[0]


@needs_docker
def test_member_raises_the_open_file_limit():
    service = next(iter(resolved(MEMBER).values()))
    assert service.get("ulimits", {}).get("nofile") == {"soft": 64000, "hard": 64000}


@needs_docker
def test_member_cache_is_raisable_without_editing():
    for env_value, expected in ((None, "0.5"), ("1.5", "1.5")):
        cmd = next(command(s) for s in resolved(MEMBER, MONGO_CACHE_GB=env_value).values())
        assert f"--wiredTigerCacheSizeGB {expected}" in cmd


@needs_docker
def test_node_stack_is_a_single_standalone_mongod():
    cmds = [command(s) for s in resolved(NODE).values()]
    assert len(cmds) == 1
    assert cmds[0].startswith("mongod ")
    assert "--port 27017" in cmds[0]
    assert "--replSet" not in cmds[0]      # standalone: no set to join
    assert "--shardsvr" not in cmds[0]
    assert "--configsvr" not in cmds[0]


@needs_docker
def test_node_raises_the_open_file_limit():
    service = next(iter(resolved(NODE).values()))
    assert service.get("ulimits", {}).get("nofile") == {"soft": 64000, "hard": 64000}


@needs_docker
def test_node_cache_is_raisable_without_editing():
    for env_value, expected in ((None, "0.5"), ("1.5", "1.5")):
        cmd = next(command(s) for s in resolved(NODE, MONGO_CACHE_GB=env_value).values())
        assert f"--wiredTigerCacheSizeGB {expected}" in cmd
