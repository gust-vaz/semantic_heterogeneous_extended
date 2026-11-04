#!/usr/bin/env python3
"""Run a whole cloud campaign, as many clusters at once as the external IP quota allows.

Experiments run detached on each control VM, so a dead laptop or a dropped SSH session
loses nothing. Every finished step is recorded under <results>/.state, and running this
again with the same arguments carries on from there. One cluster only ever runs one
experiment at a time: two at once would measure each other.
"""
import argparse
import json
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime

IP_BUDGET = 8
#: Ordered so short clusters surface a problem before the long ones are paid for.
BSERIES = ["b1", "b2", "b3", "b4"]
SSERIES = ["s1", "s3", "s4", "s5"]   # s2 rides along with s1
PLAN = {"rs5": BSERIES, "single": BSERIES, "rs3": BSERIES,
        "sh1": SSERIES, "sh4": SSERIES, "sh7": SSERIES}
DATA_VMS = {"rs5": 5, "single": 1, "rs3": 3, "sh1": 1, "sh4": 4, "sh7": 7}
ROLE = {"rs5": "member", "rs3": "member", "single": "node",
        "sh1": "shard", "sh4": "shard", "sh7": "shard"}
ZONE = os.environ.get("GCP_ZONE", "us-central1-b")
POLL_S = 120
COPY_EVERY_S = 900
BOOT_LIMIT_S = 1500
ATTEMPTS = 2


def select_plan(deployments, experiments):
    """PLAN cut down to these clusters and experiments (None keeps all), in plan order."""
    known = {experiment for steps in PLAN.values() for experiment in steps}
    unknown = sorted(set(deployments or []) - set(PLAN)) + sorted(set(experiments or []) - known)
    if unknown:
        raise ValueError(f"not in the plan: {', '.join(unknown)}")
    plan = {}
    for name, steps in PLAN.items():
        if deployments is not None and name not in deployments:
            continue
        kept = [step for step in steps if experiments is None or step in experiments]
        if kept:
            plan[name] = kept
    return plan


def ips(deployment):
    return DATA_VMS[deployment] + 1


def fitting(pending, running):
    """Which pending deployments to start now, earlier in the plan first."""
    free = IP_BUDGET - sum(ips(name) for name in running)
    chosen = []
    for name in pending:
        if ips(name) <= free:
            chosen.append(name)
            free -= ips(name)
    return chosen


class Campaign:
    def __init__(self, args):
        self.args = args
        self.plan = select_plan(getattr(args, "deployments", None),
                                getattr(args, "experiments", None))
        self.root = os.path.abspath(args.results)
        self.state_dir = os.path.join(self.root, ".state")
        os.makedirs(self.state_dir, exist_ok=True)
        self.lock = threading.Lock()
        self.provisioning = threading.Lock()

    def log(self, deployment, message):
        line = f"{datetime.now():%m-%d %H:%M:%S} [{deployment}] {message}"
        with self.lock:
            print(line, flush=True)
            with open(os.path.join(self.root, "progress.log"), "a") as handle:
                handle.write(line + "\n")

    def done(self, deployment, step):
        return os.path.exists(os.path.join(self.state_dir, f"{deployment}.{step}"))

    def mark(self, deployment, step):
        open(os.path.join(self.state_dir, f"{deployment}.{step}"), "w").close()

    def script(self, name, *arguments):
        env = {**os.environ, "REPO_BRANCH": self.args.branch, "BENCH_GIT_SHA": self.args.sha[:7]}
        return subprocess.run(["bash", os.path.join(self.args.scripts, name), *arguments],
                              env=env, capture_output=True, text=True, stdin=subprocess.DEVNULL)

    def required(self, deployment, name, *arguments):
        result = self.script(name, *arguments)
        if result.returncode != 0:
            raise RuntimeError(f"{name} {' '.join(arguments)}: {result.stderr[-800:]}")
        return result

    def ssh(self, vm, command, timeout=180):
        """(ok, stdout). A failure here is usually the network, so callers retry."""
        try:
            result = subprocess.run(
                ["gcloud", "compute", "ssh", vm, f"--zone={ZONE}", "--quiet",
                 f"--command={command}"],
                capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            return False, ""
        return result.returncode == 0, result.stdout.strip()

    def ssh_retrying(self, deployment, vm, command, limit_s=1800):
        started = time.time()
        while True:
            ok, out = self.ssh(vm, command)
            if ok:
                return out
            if time.time() - started > limit_s:
                raise RuntimeError(f"ssh {vm} kept failing: {command[:80]}")
            self.repin_ssh()
            time.sleep(30)

    def repin_ssh(self):
        """The firewall admits one public IP, and a laptop's can change mid-campaign."""
        try:
            ip = subprocess.run(["curl", "-s", "--max-time", "10", "ifconfig.me"],
                                capture_output=True, text=True).stdout.strip()
            if ip:
                subprocess.run(["gcloud", "compute", "firewall-rules", "update", "mellow-net-ssh",
                                f"--source-ranges={ip}/32"], capture_output=True, text=True)
        except OSError:
            pass

    def wait_for_boot(self, deployment, control):
        vms = [f"mellow-{deployment}-{ROLE[deployment]}{i}"
               for i in range(1, DATA_VMS[deployment] + 1)]
        started = time.time()
        while True:
            ok, _ = self.ssh(control, "docker image inspect mongo-runner:local >/dev/null 2>&1")
            ready = ok
            for vm in vms:
                if not ready:
                    break
                ok, out = self.ssh(vm, "docker ps -q")
                ready = ok and bool(out)
            if ready:
                return
            if time.time() - started > BOOT_LIMIT_S:
                raise RuntimeError("the cluster did not finish booting")
            time.sleep(30)

    def copy_results(self, deployment, control):
        dest = os.path.join(self.root, deployment)
        staging = dest + ".incoming"
        shutil.rmtree(staging, ignore_errors=True)
        result = subprocess.run(
            ["gcloud", "compute", "scp", "--recurse", f"--zone={ZONE}", "--quiet",
             f"{control}:/opt/mellow/repo/results", staging],
            capture_output=True, text=True, stdin=subprocess.DEVNULL)
        if result.returncode != 0:
            shutil.rmtree(staging, ignore_errors=True)
            return False
        shutil.rmtree(dest, ignore_errors=True)
        os.replace(staging, dest)
        return True

    def run_experiment(self, deployment, control, experiment):
        name = f"bench-{experiment}"
        inspect = (f"docker inspect -f '{{{{.State.Status}}}} {{{{.State.ExitCode}}}}' {name} "
                   f"2>/dev/null || echo absent")
        attempts = 0
        last_copy = started = time.time()
        while True:
            state = self.ssh_retrying(deployment, control, inspect).split()
            if not state or state[0] == "absent":
                attempts += 1
                self.log(deployment, f"{experiment}: starting (attempt {attempts})")
                self.required(deployment, "run.sh", deployment, "--experiment", experiment,
                              "--profile", self.args.profile, "--corpus", self.args.corpus,
                              "--detach")
            elif state[0] in ("exited", "dead"):
                code = state[1] if len(state) > 1 else "?"
                logs = self.ssh_retrying(deployment, control, f"docker logs --tail 40 {name} 2>&1")
                with open(os.path.join(self.state_dir, f"{deployment}.{experiment}.log"), "w") as handle:
                    handle.write(logs + "\n")
                if code == "0":
                    self.log(deployment, f"{experiment}: finished in "
                                         f"{(time.time() - started) / 3600:.2f} h")
                    return
                self.log(deployment, f"{experiment}: FAILED, exit {code}: {logs[-400:]}")
                if attempts >= ATTEMPTS:
                    raise RuntimeError(f"{experiment} failed with exit {code}")
                # Finished cells are skipped on the retry: the harness resumes from its CSVs.
                self.ssh_retrying(deployment, control, f"docker rm {name} >/dev/null")
                continue
            else:
                rows = self.ssh_retrying(
                    deployment, control,
                    f"cat /opt/mellow/repo/results/{experiment}/*.csv 2>/dev/null | wc -l")
                self.log(deployment, f"{experiment}: running "
                                     f"{(time.time() - started) / 60:.0f} min, "
                                     f"{max(int(rows or 0) - 1, 0)} rows")
            if time.time() - last_copy > COPY_EVERY_S:
                self.copy_results(deployment, control)
                last_copy = time.time()
            time.sleep(POLL_S)

    def pin_code(self, deployment, control):
        """So a later push cannot make clusters of one campaign run different code."""
        self.ssh_retrying(deployment, control,
                          f"sudo git -C /opt/mellow/repo fetch -q origin && "
                          f"sudo git -C /opt/mellow/repo checkout -q {self.args.sha}")

    def run_cluster(self, deployment):
        control = f"mellow-{deployment}-control"
        if not self.done(deployment, "provisioned"):
            with self.provisioning:
                self.log(deployment, "provisioning")
                self.required(deployment, "provision.sh", deployment)
            self.mark(deployment, "provisioned")
        if not self.done(deployment, "ready"):
            self.log(deployment, "waiting for the VMs to boot")
            self.wait_for_boot(deployment, control)
            self.pin_code(deployment, control)
            if deployment.startswith("rs"):
                self.required(deployment, "init-replicaset.sh", deployment)
            elif deployment.startswith("sh"):
                self.required(deployment, "init-cluster.sh", deployment)
            self.mark(deployment, "ready")
        # Again on a resume, which may name a newer commit. A running experiment has
        # already imported its code, and its rows keep the sha it was started with.
        self.pin_code(deployment, control)
        self.log(deployment, f"ready at {self.args.sha[:7]}")
        for experiment in self.plan[deployment]:
            if self.done(deployment, f"done-{experiment}"):
                continue
            self.run_experiment(deployment, control, experiment)
            if not self.copy_results(deployment, control):
                raise RuntimeError(f"could not copy the results of {experiment}")
            self.mark(deployment, f"done-{experiment}")
        self.finish(deployment)

    def finish(self, deployment):
        self.required(deployment, "teardown.sh", deployment)
        self.mark(deployment, "finished")
        self.log(deployment, "finished, VMs deleted")

    def guarded(self, deployment):
        try:
            self.run_cluster(deployment)
        except Exception as failure:
            self.log(deployment, f"CLUSTER FAILED: {failure}")
            control = f"mellow-{deployment}-control"
            self.copy_results(deployment, control)
            # Deleted anyway: the IPs are what every remaining cluster is waiting for.
            self.script("teardown.sh", deployment)
            self.mark(deployment, "failed")

    def run(self):
        pending = [name for name in self.plan if not (self.done(name, "finished") or self.done(name, "failed"))]
        running = {}
        while pending or running:
            for name, thread in list(running.items()):
                if not thread.is_alive():
                    del running[name]
            for name in fitting(pending, list(running)):
                pending.remove(name)
                running[name] = threading.Thread(target=self.guarded, args=(name,), daemon=True)
                running[name].start()
            time.sleep(10)
        failed = [name for name in self.plan if self.done(name, "failed")]
        self.script("teardown.sh", "sh7", "--all")
        with open(os.path.join(self.root, "summary.json"), "w") as handle:
            json.dump({"sha": self.args.sha, "failed": failed}, handle)
        self.log("campaign", f"DONE, failed: {failed or 'none'}")
        return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, help="Where results and state are kept")
    parser.add_argument("--sha", required=True, help="Full commit every cluster runs")
    parser.add_argument("--branch", required=True, help="Pushed branch containing that commit")
    parser.add_argument("--scripts", default=os.path.dirname(os.path.abspath(__file__)))
    parser.add_argument("--profile", default="medium")
    parser.add_argument("--corpus", default="synthetic")
    parser.add_argument("--deployments", type=lambda text: text.split(","), default=None,
                        help="Comma-separated clusters to run (default: the whole plan)")
    parser.add_argument("--experiments", type=lambda text: text.split(","), default=None,
                        help="Comma-separated experiments to run on them (default: all)")
    return Campaign(parser.parse_args()).run()


if __name__ == "__main__":
    raise SystemExit(main())
