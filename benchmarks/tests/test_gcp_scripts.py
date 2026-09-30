"""The deploy/gcp/ bash scripts, exercised offline.

Every gcloud-touching script has a --dry-run that prints its commands through a
run() wrapper instead of executing, and a fake gcloud on PATH keeps even the
non-dry-run guards (quota, preflight) away from real GCP. No test here creates a
VM or spends a cent.
"""
import os
import shutil
import subprocess

import pytest

GCP = "deploy/gcp"


def read(name):
    with open(os.path.join(GCP, name)) as handle:
        return handle.read()


def run(name, *args, env=None):
    return subprocess.run(["bash", os.path.join(GCP, name), *args],
                          capture_output=True, text=True, env=env)


def with_fake_gcloud(tmp_path, exit_code=0, stdout=""):
    """A gcloud stub on PATH so preflight/dry-run tests never touch real GCP."""
    stub = tmp_path / "gcloud"
    stub.write_text("#!/usr/bin/env bash\n"
                    f"printf '%s' {stdout!r}\n"
                    f"exit {exit_code}\n")
    stub.chmod(0o755)
    env = dict(os.environ)
    env["PATH"] = f"{tmp_path}:{env['PATH']}"
    return env


SCRIPTS = ["preflight.sh", "provision.sh", "init-cluster.sh",
           "run.sh", "teardown.sh", "stop.sh", "start.sh", "status.sh"]


@pytest.mark.parametrize("name", SCRIPTS)
def test_every_script_is_executable_and_strict(name):
    assert os.access(os.path.join(GCP, name), os.X_OK)
    assert "set -euo pipefail" in read(name)


@pytest.mark.parametrize("name", SCRIPTS)
def test_every_script_has_help(name):
    done = run(name, "--help")
    assert done.returncode == 0
    assert "Usage" in done.stdout


@pytest.mark.skipif(shutil.which("shellcheck") is None, reason="shellcheck not available")
@pytest.mark.parametrize("name", SCRIPTS)
def test_shellcheck_is_clean(name):
    done = subprocess.run(["shellcheck", "-x", os.path.join(GCP, name)],
                          capture_output=True, text=True)
    assert done.returncode == 0, done.stdout


def test_preflight_fails_when_gcloud_is_missing(tmp_path):
    # A PATH with bash and coreutils but no gcloud SDK dir: the shell still works,
    # `command -v gcloud` does not. (Setting PATH to tmp_path alone would also hide
    # bash and fail to launch the script at all.)
    bash_dir = os.path.dirname(shutil.which("bash"))
    env = dict(os.environ)
    env["PATH"] = f"{tmp_path}:{bash_dir}"
    done = run("preflight.sh", env=env)
    assert done.returncode != 0
    assert "gcloud" in (done.stdout + done.stderr)


def test_preflight_flags_a_missing_project_with_its_remedy(tmp_path):
    # authenticated (auth list returns an account) but no project set.
    stub = tmp_path / "gcloud"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'case "$*" in\n'
        '  *"auth list"*) echo me@example.com ;;\n'
        '  *"config get-value project"*) echo "" ;;\n'
        '  *) echo "" ;;\n'
        'esac\n'
        "exit 0\n")
    stub.chmod(0o755)
    env = dict(os.environ)
    env["PATH"] = f"{tmp_path}:{env['PATH']}"
    done = run("preflight.sh", env=env)
    assert done.returncode != 0
    assert "config set project" in (done.stdout + done.stderr)


def test_provision_dry_run_sh8_plans_eight_shards_and_one_control(tmp_path):
    env = with_fake_gcloud(tmp_path)
    done = run("provision.sh", "sh8", "--dry-run", env=env)
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert out.count("instances create") == 9          # 8 shard + 1 control
    assert "deployment=sh8" in out
    assert out.count("--machine-type=e2-small") == 8
    assert out.count("--machine-type=n2-standard-4") == 1
    assert "firewall-rules create" in out
    assert "27017-27019" in out


def test_provision_dry_run_sh4_plans_four_shards(tmp_path):
    done = run("provision.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0
    assert done.stdout.count("instances create") == 5


def test_provision_refuses_an_unknown_deployment(tmp_path):
    done = run("provision.sh", "rs3", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode != 0
    assert "rs3" in (done.stdout + done.stderr)


def test_provision_refuses_when_quota_is_short(tmp_path):
    # gcloud reports a CPUS quota of 4, far under sh8's need; provision must stop
    # before creating anything.
    stub = tmp_path / "gcloud"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'case "$*" in\n'
        '  *"regions describe"*) echo 4 ;;\n'
        '  *"auth list"*) echo me@example.com ;;\n'
        '  *"config get-value project"*) echo proj ;;\n'
        '  *) echo "" ;;\n'
        'esac\n'
        "exit 0\n")
    stub.chmod(0o755)
    env = dict(os.environ); env["PATH"] = f"{tmp_path}:{env['PATH']}"
    done = run("provision.sh", "sh8", env=env)   # not dry-run: exercises the guard
    assert done.returncode != 0
    assert "quota" in (done.stdout + done.stderr).lower()


def test_init_cluster_dry_run_collects_ips_and_runs_the_init(tmp_path):
    done = run("init-cluster.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert "instances list" in out            # collects shard private IPs
    assert "SHARD_HOSTS" in out               # builds the env contract
    assert "init-sharded.js" in out           # runs the parameterized init


def test_run_dry_run_sets_router_uri_and_calls_the_module(tmp_path):
    done = run("run.sh", "sh4", "--profile", "smoke", "--dry-run",
               env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert "BENCH_ROUTER_URI=" in out
    assert "benchmarks.experiments.s1_operation_cost" in out
    assert "--deployment sh4" in out
    assert "--profile smoke" in out
    assert "compute scp" in out                      # brings results back


def test_run_passes_a_bounded_server_selection_timeout(tmp_path):
    # A wrong BENCH_ROUTER_URI must fail fast, not hang on default selection.
    done = run("run.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert "serverSelectionTimeoutMS" in done.stdout


def test_teardown_dry_run_deletes_named_vms_not_by_filter(tmp_path):
    done = run("teardown.sh", "sh8", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    # the list selects by label...
    assert "labels.deployment=sh8" in out
    # ...and the delete targets the resulting VM names. `gcloud compute instances
    # delete` takes names, not --filter; carrying --filter here would fail for real.
    delete_line = next(line for line in out.splitlines() if "instances delete" in line)
    assert "mellow-sh8" in delete_line
    assert "--filter" not in delete_line


def test_teardown_is_a_noop_when_nothing_matches(tmp_path):
    # gcloud lists no instances -> teardown succeeds with a clear message.
    stub = tmp_path / "gcloud"
    stub.write_text("#!/usr/bin/env bash\necho ''\nexit 0\n")
    stub.chmod(0o755)
    env = dict(os.environ); env["PATH"] = f"{tmp_path}:{env['PATH']}"
    done = run("teardown.sh", "sh4", env=env)
    assert done.returncode == 0
    assert "nothing" in done.stdout.lower()


def test_stop_and_start_and_status_dry_run(tmp_path):
    env = with_fake_gcloud(tmp_path)
    assert "instances stop" in run("stop.sh", "sh4", "--dry-run", env=env).stdout
    assert "instances start" in run("start.sh", "sh4", "--dry-run", env=env).stdout
    assert "instances list" in run("status.sh", "--dry-run", env=env).stdout
