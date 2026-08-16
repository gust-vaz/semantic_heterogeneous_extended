"""The replica-set init scripts must be safe to re-run.

`runner` depends on `mongo-init` completing successfully, so a non-idempotent
init script makes every `docker compose run runner` fail against a replica set
that is already initialised - which is exactly what happens when a user already
has a stack up and then invokes ./bench.sh.
"""

import shutil
import subprocess

import pytest

SCRIPTS = [
    "docker/replicaset/init-replicaset.js",
    "docker/replicaset/init-replicaset5.js",
]

needs_docker = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker not available"
)


@pytest.mark.parametrize("script", SCRIPTS)
def test_init_script_checks_for_an_existing_replica_set(script):
    with open(script) as handle:
        text = handle.read()
    # It must decide up front whether the set is already initiated, rather than
    # calling rs.initiate() blindly and retrying until it gives up.
    assert "rs.status()" in text
    assert "already initialized" in text


@pytest.mark.parametrize("script", SCRIPTS)
def test_init_script_treats_already_initialized_as_success(script):
    with open(script) as handle:
        text = handle.read()
    # The 'already initialized' branch must set initiated = true, not fall
    # through to the retry/quit(1) path.
    marker = text.split("already initialized")[1]
    assert "initiated = true" in marker.split("quit(1)")[0]


@needs_docker
@pytest.mark.slow
def test_rerunning_init_against_a_live_replica_set_succeeds():
    """Regression: this exited 1 after ~120s of 'already initialized' retries."""
    compose = "docker-compose.replicaset.yml"
    up = subprocess.run(["docker", "compose", "-f", compose, "up", "-d"],
                        capture_output=True, text=True)
    assert up.returncode == 0, up.stderr

    # Re-run the init script against the now-initialised set.
    first = subprocess.run(
        ["docker", "compose", "-f", compose, "run", "--rm", "mongo-init"],
        capture_output=True, text=True, timeout=180)
    assert first.returncode == 0, f"init not idempotent:\n{first.stdout}\n{first.stderr}"
