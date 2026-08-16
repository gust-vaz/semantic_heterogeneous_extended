import os
import re
import subprocess

SCRIPT = "bench.sh"


def _read():
    with open(SCRIPT) as handle:
        return handle.read()


def test_script_exists_and_is_executable():
    assert os.path.exists(SCRIPT)
    assert os.access(SCRIPT, os.X_OK)


def test_help_lists_every_experiment_and_exits_zero():
    done = subprocess.run(["bash", SCRIPT, "--help"], capture_output=True, text=True)
    assert done.returncode == 0
    for token in ["b1", "b3", "b4", "b6", "all", "--profile", "--deployments",
                  "--keep-going", "--corpus"]:
        assert token in done.stdout


def test_unknown_experiment_fails_with_a_clear_message():
    done = subprocess.run(["bash", SCRIPT, "b9"], capture_output=True, text=True)
    assert done.returncode != 0
    assert "b9" in (done.stdout + done.stderr)


def test_missing_experiment_argument_fails():
    done = subprocess.run(["bash", SCRIPT], capture_output=True, text=True)
    assert done.returncode != 0


def test_default_deployment_sweeps_match_the_spec():
    text = _read()
    assert re.search(r"b1\)\s*echo\s+\"single,rs3,rs5\"", text)
    assert re.search(r"b6\)\s*echo\s+\"single,rs3\"", text)


def test_script_passes_the_git_sha_into_the_container():
    assert "BENCH_GIT_SHA" in _read()


def test_script_tears_down_with_volumes_and_traps_interrupts():
    text = _read()
    assert "down -v" in text
    assert "trap" in text


def test_script_uses_strict_bash_mode():
    assert "set -euo pipefail" in _read()


def test_script_checks_for_docker_before_running_anything():
    text = _read()
    assert "command -v docker" in text
    assert "docker compose version" in text
