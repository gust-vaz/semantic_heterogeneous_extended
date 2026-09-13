"""Compose stacks as Compose itself resolves them, for tests.

Asserting on `docker compose config` rather than on a file's text is what catches a
YAML anchor that looks like it applies to a service and does not, or a variable that
is never substituted. Profiled services (the `runner`) are left out of the resolved
config unless their profile is named - measured on Compose v5.3.1 - so a test that
needs one has to pass `profiles`.
"""
import json
import os
import shutil
import subprocess

import pytest

needs_docker = pytest.mark.skipif(
    shutil.which("docker") is None, reason="docker not available"
)


def resolved(compose, profiles=(), **env):
    """Services of `compose` as Compose resolves them, under exactly `env`.

    Variables the stacks read are removed from the inherited environment first, so
    a value exported in the developer's shell cannot leak into a default-value test.
    """
    base = {k: v for k, v in os.environ.items()
            if k not in ("MONGO_CACHE_GB", "MONGOS_HOST_PORT")}
    base.update({k: v for k, v in env.items() if v is not None})
    cmd = ["docker", "compose", "-f", compose]
    for profile in profiles:
        cmd += ["--profile", profile]
    out = subprocess.run([*cmd, "config", "--format", "json"],
                         capture_output=True, text=True, timeout=60, env=base)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)["services"]


def command(service):
    cmd = service.get("command") or ""
    return " ".join(cmd) if isinstance(cmd, list) else cmd
