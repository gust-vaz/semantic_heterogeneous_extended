"""The cloud replica-set init script, exercised with mongosh --nodb.

It never reaches a live set here: the member-array construction is asserted from
what the script prints before connecting, and the required-input guard quits
before any connection. Unroutable TEST-NET-1 addresses (RFC 5737) with a single
connect attempt keep the connect failure fast and deterministic.
"""
import os
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.skipif(shutil.which("mongosh") is None,
                                reason="mongosh not available")
SCRIPT = "docker/replicaset/init-replicaset-cloud.js"


def run(env_extra, timeout=30):
    env = {**os.environ, **env_extra}
    return subprocess.run(["mongosh", "--nodb", SCRIPT],
                          capture_output=True, text=True, env=env, timeout=timeout)


def test_requires_replset_hosts():
    done = run({"REPLSET_HOSTS": ""})
    assert done.returncode != 0
    assert "REPLSET_HOSTS" in done.stdout


def test_builds_members_with_a_deterministic_primary():
    done = run({"REPLSET_HOSTS": "192.0.2.1:27017,192.0.2.2:27017,192.0.2.3:27017",
                "REPLSET_CONNECT_ATTEMPTS": "1"})
    printed = done.stdout.replace(" ", "")
    assert '"_id":"rs0"' in printed
    assert '"priority":2' in printed            # exactly one member leads
    assert printed.count('"priority":2') == 1
    assert printed.count('"priority":1') == 2    # the other two are secondaries
