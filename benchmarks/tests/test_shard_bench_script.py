import os
import re
import subprocess

SCRIPT = "shard-bench.sh"


def _read():
    with open(SCRIPT) as handle:
        return handle.read()


def _run(*args):
    return subprocess.run(["bash", SCRIPT, *args], capture_output=True, text=True)


def test_script_exists_and_is_executable():
    assert os.path.exists(SCRIPT)
    assert os.access(SCRIPT, os.X_OK)


def test_help_lists_every_experiment_and_exits_zero():
    done = _run("--help")
    assert done.returncode == 0
    for token in ["s1", "s2", "s3", "s4", "s5", "all", "--profile",
                  "--deployments", "--chunk-size-mb", "--fresh"]:
        assert token in done.stdout


def test_help_never_offers_a_replica_set_deployment():
    # the B series' deployments are not this script's to run
    for token in ["rs3", "rs5", "single"]:
        assert token not in _run("--help").stdout


def test_a_b_series_experiment_is_refused():
    done = _run("b1")
    assert done.returncode != 0
    assert "Unknown experiment" in done.stderr


def test_a_replica_set_deployment_is_refused():
    done = _run("s1", "--deployments", "rs3")
    assert done.returncode != 0
    assert "Unknown deployment 'rs3'" in done.stderr


def test_an_absolute_out_path_is_refused_before_anything_starts():
    """Same footgun as bench.sh: --out is read inside the runner container,
    where the repo is mounted at /app, so an absolute host path is written in
    the container and lost with it - discarding the campaign's resume state."""
    done = _run("s1", "--out", "/tmp/somewhere")
    assert done.returncode != 0
    assert "/tmp/somewhere" in done.stderr
    assert "container" in done.stderr


def test_every_experiment_sweeps_all_three_shard_counts():
    assert re.search(r's1\|s2\|s3\|s4\|s5\)\s*echo\s+"sh1,sh4,sh8"', _read())


def test_all_does_not_run_s2_separately():
    # s2 rides along on s1's cells rather than building its own corpora
    assert re.search(r'EXPERIMENTS="s1 s3 s4 s5"', _read())


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


def test_script_passes_the_git_sha_into_the_container():
    assert "BENCH_GIT_SHA" in _read()
