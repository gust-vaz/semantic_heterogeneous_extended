"""The sharded stacks, asserted on what Compose and MongoDB actually do rather than
on the text of the files.

The fast tests read `docker compose config`, which is the stack as Compose itself
resolves it - YAML anchors merged, variables substituted. That is what catches an
anchor that looks like it applies to a service and does not. The slow test brings
a stack up and checks the cluster it produces.
"""
import os
import shutil
import subprocess

import pytest

from benchmarks.tests.compose_config import command, needs_docker, resolved


@pytest.mark.skipif(shutil.which("mongosh") is None, reason="mongosh not available")
def test_init_rejects_a_shard_host_list_that_does_not_match_shard_count():
    # A short SHARD_HOSTS would otherwise register the wrong shards silently.
    env = {**os.environ, "SHARD_COUNT": "4", "SHARD_HOSTS": "a:1,b:2"}
    done = subprocess.run(["mongosh", "--nodb", "docker/sharded/init-sharded.js"],
                          capture_output=True, text=True, env=env, timeout=30)
    assert done.returncode != 0
    assert "SHARD_HOSTS" in done.stdout

COMPOSES = {"docker-compose.shard1.yml": 1,
            "docker-compose.shard4.yml": 4,
            "docker-compose.shard8.yml": 8}


@needs_docker
@pytest.mark.parametrize("compose,shards", COMPOSES.items())
def test_stack_is_one_config_server_n_shards_and_one_router(compose, shards):
    commands = [command(s) for s in resolved(compose).values()]
    assert sum("--configsvr" in c for c in commands) == 1
    assert sum("--shardsvr" in c for c in commands) == shards
    assert sum(c.startswith("mongos ") for c in commands) == 1


@needs_docker
@pytest.mark.parametrize("compose", COMPOSES)
def test_only_the_router_publishes_a_host_port(compose):
    # MellowDB only ever talks to the router; a published shard port would invite
    # clients to bypass it.
    services = resolved(compose)
    assert {name for name, s in services.items() if s.get("ports")} == {"mongos"}
    assert [p["published"] for p in services["mongos"]["ports"]] == ["27017"]


@needs_docker
@pytest.mark.parametrize("compose", COMPOSES)
def test_router_host_port_can_be_moved_without_editing_the_file(compose):
    services = resolved(compose, MONGOS_HOST_PORT="28117")
    assert [p["published"] for p in services["mongos"]["ports"]] == ["28117"]


@needs_docker
@pytest.mark.parametrize("compose", COMPOSES)
def test_every_mongod_and_mongos_raises_the_open_file_limit(compose):
    # Regression, observed during Task 6: a sharded cluster without this ran at the
    # Docker default soft limit of 1024 fds. The full test suite pushed one shard
    # past 1500 open files; it logged 18609 "Too many open files", then WiredTiger
    # could not fsync its directory and the process aborted (SIGSEGV).
    servers = {name: s for name, s in resolved(compose).items()
               if command(s).startswith(("mongod ", "mongos "))}
    assert servers
    for name, service in servers.items():
        assert service.get("ulimits", {}).get("nofile") == {"soft": 64000, "hard": 64000}, name


@needs_docker
@pytest.mark.parametrize("compose", COMPOSES)
def test_cache_size_is_small_by_default_and_raisable_without_editing(compose):
    # The development machine needs a small cache to fit sh8; a benchmark machine
    # must be able to raise it from the environment.
    for env_value, expected in ((None, "0.25"), ("1.5", "1.5")):
        mongods = [command(s) for s in resolved(compose, MONGO_CACHE_GB=env_value).values()
                   if command(s).startswith("mongod ")]
        assert mongods
        assert all(f"--wiredTigerCacheSizeGB {expected}" in c for c in mongods), (env_value, mongods)


@needs_docker
@pytest.mark.slow
@pytest.mark.parametrize("compose_file,shards,host_port", [
    ("docker-compose.shard1.yml", 1, "28116"),
    ("docker-compose.shard4.yml", 4, "28117"),
    ("docker-compose.shard8.yml", 8, "28118"),
])
def test_stack_comes_up_as_a_cluster_and_init_can_be_rerun(compose_file, shards, host_port):
    """The stack becomes an N-shard cluster behind a router, the shards really run
    with the raised fd limit, and running the init container again against the
    live cluster succeeds without changing it.

    Runs under its own project name and host port, so `down -v` can only ever
    remove this test's own volumes - never a corpus loaded into a real stack.
    Measured on the development machine: sh8's ten containers take about 1 GB at
    idle with the default cache and initialise in about 12 s, so both stacks are
    cheap enough to bring up here.
    """
    compose = ["docker", "compose", "-p", f"mellowtest-sh{shards}", "-f", compose_file]
    env = {**os.environ, "MONGOS_HOST_PORT": host_port}

    def run(*args, timeout=300):
        return subprocess.run([*compose, *args], capture_output=True, text=True,
                              timeout=timeout, env=env)

    def on_router(js):
        result = run("exec", "-T", "mongos", "mongosh", "--quiet", "--eval", js)
        assert result.returncode == 0, result.stderr
        return result.stdout.strip().splitlines()[-1]

    try:
        up = run("up", "-d", timeout=600)
        assert up.returncode == 0, up.stderr
        # `compose wait` exits with the container's own exit code.
        init = run("wait", "mongo-init", timeout=600)
        assert init.returncode == 0, f"init failed:\n{run('logs', 'mongo-init').stdout}"

        assert on_router('db.adminCommand("hello").msg') == "isdbgrid"
        assert on_router('db.getSiblingDB("config").shards.countDocuments({})') == str(shards)
        # The last shard, so a fd limit that only reached the first services fails here.
        last = f"shard{shards}"
        assert run("exec", "-T", last, "sh", "-c", "ulimit -Sn").stdout.strip() == "64000"

        again = run("run", "--rm", "mongo-init")
        assert again.returncode == 0, f"init not idempotent:\n{again.stdout}\n{again.stderr}"
        assert on_router('db.getSiblingDB("config").shards.countDocuments({})') == str(shards)
    finally:
        run("down", "-v", timeout=300)


@needs_docker
@pytest.mark.slow
def test_init_registers_shards_at_addresses_from_the_environment():
    """The env path is what the cloud uses: with SHARD_HOSTS/CONFIG_HOST set, the
    shardAddress() env branch runs and still produces a healthy N-shard cluster.
    Proven locally with the container service names (the only addresses that
    resolve here), so no cloud is needed."""
    compose = ["docker", "compose", "-p", "mellowtest-shenv",
               "-f", "docker-compose.shard4.yml"]
    env = {**os.environ, "MONGOS_HOST_PORT": "28119"}

    def run(*args, timeout=300):
        return subprocess.run([*compose, *args], capture_output=True, text=True,
                              timeout=timeout, env=env)

    def on_router(js):
        r = run("exec", "-T", "mongos", "mongosh", "--quiet", "--eval", js)
        assert r.returncode == 0, r.stderr
        return r.stdout.strip().splitlines()[-1]

    try:
        assert run("up", "-d", timeout=600).returncode == 0
        assert run("wait", "mongo-init", timeout=600).returncode == 0
        hosts = "shard1:27018,shard2:27018,shard3:27018,shard4:27018"
        again = run("run", "--rm",
                    "-e", "SHARD_HOSTS=" + hosts,
                    "-e", "CONFIG_HOST=cfg1:27019",
                    "-e", "ROUTER_HOST=mongos:27017",
                    "mongo-init")
        assert again.returncode == 0, again.stdout + again.stderr
        registered = on_router(
            'db.getSiblingDB("config").shards.find().toArray()'
            '.map(function(s){return s.host;}).join(";")')
        assert "shard1:27018" in registered
        assert "shard4:27018" in registered
    finally:
        run("down", "-v", timeout=300)
