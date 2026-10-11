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


def sourced(expr):
    """Evaluate a config.env helper the way the scripts do."""
    done = subprocess.run(["bash", "-c", f"source {GCP}/config.env && {expr}"],
                          capture_output=True, text=True)
    return done


def test_config_maps_deployment_kind_count_and_role():
    def out(expr):
        done = sourced(expr)
        assert done.returncode == 0, done.stderr
        return done.stdout.strip()
    assert out("kind_for sh4") == "shard"
    assert out("kind_for rs3") == "replicaset"
    assert out("kind_for rs5") == "replicaset"
    assert out("data_count_for rs3") == "3"
    assert out("data_count_for rs5") == "5"
    assert out("data_count_for sh4") == "4"
    assert out("data_role_for rs5") == "member"
    assert out("data_role_for sh4") == "shard"
    assert out("kind_for single") == "standalone"
    assert out("data_count_for single") == "1"
    assert out("data_role_for single") == "node"
    assert out("shard_count_for sh8") == "8"   # kept as an alias


def test_config_rejects_an_unknown_deployment():
    done = sourced("kind_for bogus")
    assert done.returncode != 0
    assert "bogus" in done.stderr


def test_member_machine_defaults_to_e2_small():
    done = sourced('echo "$MEMBER_MACHINE"')
    assert done.stdout.strip() == "e2-small"


def test_bootstrap_member_is_strict_and_runs_the_member_compose():
    text = read("bootstrap-member.sh")
    assert "set -euo pipefail" in text
    assert "compose.member.yml" in text
    assert "mongo-cache-gb" in text


def test_bootstrap_node_is_strict_and_runs_the_node_compose():
    text = read("bootstrap-node.sh")
    assert "set -euo pipefail" in text
    assert "compose.node.yml" in text
    assert "mongo-cache-gb" in text


SCRIPTS = ["preflight.sh", "provision.sh", "init-cluster.sh", "init-replicaset.sh",
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
    assert out.count("--machine-type=e2-standard-4") == 1
    assert "firewall-rules create" in out
    assert "27017-27019" in out


def test_provision_dry_run_sh4_plans_four_shards(tmp_path):
    done = run("provision.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0
    assert done.stdout.count("instances create") == 5


def test_provision_dry_run_sh7_fits_the_eight_ip_trial_cap(tmp_path):
    # 7 shards + 1 control = 8 VMs = 8 external IPs, the trial's IN_USE_ADDRESSES cap.
    done = run("provision.sh", "sh7", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    assert done.stdout.count("instances create") == 8
    assert done.stdout.count("--machine-type=e2-small") == 7


def test_provision_refuses_an_unknown_deployment(tmp_path):
    done = run("provision.sh", "rs9", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode != 0
    assert "rs9" in (done.stdout + done.stderr)


def test_provision_dry_run_rs3_plans_three_members_and_one_control(tmp_path):
    done = run("provision.sh", "rs3", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert out.count("instances create") == 4           # 3 members + 1 control
    assert out.count("role=member") == 3
    assert "role=control" in out
    assert "bootstrap-member.sh" in out
    assert "compose-member=" in out
    assert "kind=replicaset" in out
    assert "deployment=rs3" in out
    assert out.count("--machine-type=e2-small") == 3
    assert out.count("--machine-type=e2-standard-4") == 1


def test_provision_dry_run_rs5_plans_five_members(tmp_path):
    done = run("provision.sh", "rs5", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    assert done.stdout.count("instances create") == 6
    assert done.stdout.count("role=member") == 5


def test_provision_repins_ssh_to_the_current_ip(tmp_path):
    done = run("provision.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    update_line = next(l for l in done.stdout.splitlines()
                       if "firewall-rules update" in l and "-ssh" in l)
    assert "--source-ranges=" in update_line


def test_provision_dry_run_shard_still_plans_shards_and_a_config_control(tmp_path):
    # Regression: generalising to rs must not change the shard path.
    done = run("provision.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    out = done.stdout
    assert out.count("instances create") == 5
    assert out.count("role=shard") == 4
    assert "kind=shard" in out
    assert "bootstrap-shard.sh" in out


def test_bootstrap_control_starts_the_router_only_for_shards():
    text = read("bootstrap-control.sh")
    assert "compose.control.yml" in text
    assert '"$KIND" = shard' in text


def test_provision_refuses_when_quota_is_short(tmp_path):
    # gcloud reports a CPUS quota of 4, far under sh8's need; provision must stop
    # before creating anything.
    stub = tmp_path / "gcloud"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'case "$*" in\n'
        '  *"regions describe"*) echo "CPUS,4.0" ;;\n'   # flatten+csv output
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


def test_preflight_passes_with_a_ready_account(tmp_path):
    # A realistic gcloud: authed, project set, billing True, compute enabled, and a
    # 200-CPU region. Reproduces the parsing bugs (beta billing, quota firstof) that
    # a bare-number stub hid: both must parse from real command output.
    stub = tmp_path / "gcloud"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'case "$*" in\n'
        '  *"auth list"*) echo me@example.com ;;\n'
        '  *"config get-value project"*) echo proj ;;\n'
        '  *"billing projects describe"*) echo True ;;\n'
        '  *"services list"*) echo compute.googleapis.com ;;\n'
        '  *"regions describe"*) echo "CPUS,200.0" ;;\n'
        '  *) echo "" ;;\n'
        'esac\n'
        "exit 0\n")
    stub.chmod(0o755)
    env = dict(os.environ); env["PATH"] = f"{tmp_path}:{env['PATH']}"
    done = run("preflight.sh", "--deployment", "sh8", env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert "vCPU quota (200 >= 20" in done.stdout


def test_init_cluster_dry_run_collects_ips_and_runs_the_init(tmp_path):
    done = run("init-cluster.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert "instances list" in out            # collects shard private IPs
    assert "SHARD_HOSTS" in out               # builds the env contract
    assert "init-sharded.js" in out           # runs the parameterized init


def test_init_replicaset_dry_run_collects_ips_and_runs_the_init(tmp_path):
    done = run("init-replicaset.sh", "rs3", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert "labels.role=member" in out          # collects member private IPs
    assert "REPLSET_HOSTS" in out                # builds the env contract
    assert "init-replicaset-cloud.js" in out     # runs the parameterized cloud init
    assert ":27017" in out                       # members speak on 27017


def test_init_replicaset_refuses_a_shard_deployment(tmp_path):
    done = run("init-replicaset.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode != 0


def test_run_dry_run_sets_router_uri_and_calls_the_module(tmp_path):
    done = run("run.sh", "sh4", "--profile", "smoke", "--dry-run",
               env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert "BENCH_ROUTER_URI=" in out
    assert "benchmarks.experiments.s1_operation_cost" in out
    assert "--deployment sh4" in out
    assert "--profile smoke" in out
    assert "BENCH_GIT_SHA" in out                     # provenance: git sha (computed locally)
    assert "compute scp" in out                      # brings results back
    assert "results-cloud/sh4" in out                # per-deployment dest, no nesting across runs


def test_run_passes_a_bounded_server_selection_timeout(tmp_path):
    # A wrong BENCH_ROUTER_URI must fail fast, not hang on default selection.
    done = run("run.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert "serverSelectionTimeoutMS" in done.stdout


def test_run_dry_run_rs3_b1_sets_override_uris_and_calls_the_module(tmp_path):
    done = run("run.sh", "rs3", "--experiment", "b1", "--profile", "smoke",
               "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    assert "BENCH_PRIMARY_URI=" in out
    assert "replicaSet=rs0" in out
    assert "BENCH_SECONDARY_URIS=" in out
    assert "directConnection=true" in out
    assert "benchmarks.experiments.b1_read_offloading" in out
    assert "--deployment rs3" in out
    assert "--profile smoke" in out
    assert "results-cloud/rs3" in out
    assert "BENCH_GIT_SHA" in out
    assert "serverSelectionTimeoutMS" in out
    assert "BENCH_ROUTER_URI" not in out     # rs uses the primary/secondary override


def test_run_maps_every_b_experiment(tmp_path):
    env = with_fake_gcloud(tmp_path)
    for exp, module in [("b1", "b1_read_offloading"), ("b2", "b2_write_replication"),
                        ("b3", "b3_strategy_distribution"), ("b4", "b4_chain_depth")]:
        out = run("run.sh", "rs3", "--experiment", exp, "--dry-run", env=env).stdout
        assert f"benchmarks.experiments.{module}" in out


def test_run_rs_defaults_to_b1(tmp_path):
    out = run("run.sh", "rs3", "--dry-run", env=with_fake_gcloud(tmp_path)).stdout
    assert "b1_read_offloading" in out


def test_run_secondary_uris_exclude_the_primary_member(tmp_path):
    # rs3: 3 members, member1 leads -> exactly 2 secondary URIs.
    out = run("run.sh", "rs3", "--experiment", "b1", "--dry-run",
              env=with_fake_gcloud(tmp_path)).stdout
    line = next(l for l in out.splitlines() if "BENCH_SECONDARY_URIS=" in l)
    assert line.count("directConnection=true") == 2


def test_run_shard_path_is_unchanged(tmp_path):
    # Regression: sh4 still uses the router URI and the s1 default module.
    out = run("run.sh", "sh4", "--dry-run", env=with_fake_gcloud(tmp_path)).stdout
    assert "BENCH_ROUTER_URI=" in out
    assert "benchmarks.experiments.s1_operation_cost" in out
    assert "BENCH_PRIMARY_URI" not in out


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


def test_teardown_dry_run_rs5_names_members_and_deletes_the_vpc(tmp_path):
    done = run("teardown.sh", "rs5", "--all", "--dry-run", env=with_fake_gcloud(tmp_path))
    assert done.returncode == 0, done.stderr
    out = done.stdout
    delete_line = next(l for l in out.splitlines() if "instances delete" in l)
    assert "mellow-rs5-control" in delete_line
    assert "mellow-rs5-member5" in delete_line
    assert "networks delete" in out        # --all removes the VPC


def test_teardown_all_removes_the_vpc_even_with_no_vms(tmp_path):
    # gcloud lists no instances; --all must still delete the VPC, not early-exit.
    stub = tmp_path / "gcloud"
    stub.write_text("#!/usr/bin/env bash\necho ''\nexit 0\n")
    stub.chmod(0o755)
    env = dict(os.environ); env["PATH"] = f"{tmp_path}:{env['PATH']}"
    done = run("teardown.sh", "rs3", "--all", env=env)
    assert done.returncode == 0, done.stderr
    assert "nothing to delete" in done.stdout.lower()
    assert "Removed the VPC" in done.stdout


def test_stop_and_start_and_status_dry_run(tmp_path):
    env = with_fake_gcloud(tmp_path)
    assert "instances stop" in run("stop.sh", "sh4", "--dry-run", env=env).stdout
    assert "instances start" in run("start.sh", "sh4", "--dry-run", env=env).stdout
    assert "instances list" in run("status.sh", "--dry-run", env=env).stdout


def test_stop_start_accept_a_replica_set(tmp_path):
    env = with_fake_gcloud(tmp_path)
    assert run("stop.sh", "rs3", "--dry-run", env=env).returncode == 0
    assert run("start.sh", "rs5", "--dry-run", env=env).returncode == 0


def test_preflight_quota_for_a_replica_set(tmp_path):
    stub = tmp_path / "gcloud"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'case "$*" in\n'
        '  *"auth list"*) echo me@example.com ;;\n'
        '  *"config get-value project"*) echo proj ;;\n'
        '  *"billing projects describe"*) echo True ;;\n'
        '  *"services list"*) echo compute.googleapis.com ;;\n'
        '  *"regions describe"*) echo "CPUS,200.0" ;;\n'
        '  *) echo "" ;;\n'
        'esac\nexit 0\n')
    stub.chmod(0o755)
    env = dict(os.environ); env["PATH"] = f"{tmp_path}:{env['PATH']}"
    done = run("preflight.sh", "--deployment", "rs5", env=env)
    assert done.returncode == 0, done.stdout + done.stderr
    assert "vCPU quota (200 >= 14" in done.stdout   # 4 + 5*2
