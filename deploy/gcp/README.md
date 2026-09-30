# Running the S-series on a real distributed cluster (GCP)

A thin bash-over-`gcloud` layer that runs the sharding benchmarks (série S) on a
**real multi-VM cluster** — one single-node shard per VM, a config server and a
router on a control VM — instead of the single-host Docker simulation. The
`benchmarks/` harness stays cloud-agnostic; everything GCP-specific lives here.

Design: `docs/superpowers/specs/2026-09-30-cloud-distributed-benchmarks-design.md`.

> **Dry-run everything first.** Every gcloud-touching script takes `--dry-run`,
> which prints the exact `gcloud` commands without creating anything. Use it to
> eyeball VM counts, labels and firewall rules before spending a cent.

## First-time setup (interactive — done by you)

The scripts never touch your account until you have done these once. `gcloud` is
not automatable for the sign-in and billing steps, so they are yours:

1. **Install the Google Cloud SDK** (Linux): follow
   <https://cloud.google.com/sdk/docs/install>, or your distro's package.
2. **Sign in.** In the Claude Code prompt, run it so the output returns to the
   session:
   ```
   ! gcloud auth login
   ```
3. **Pick a project and link billing** (the trial billing account):
   ```
   gcloud projects create <PROJECT_ID>            # or reuse an existing one
   gcloud config set project <PROJECT_ID>
   gcloud billing projects link <PROJECT_ID> --billing-account <ACCOUNT_ID>
   ```
   (Linking billing is often easiest in the console: Billing → Account
   management → link the trial account to the project.)
4. **Set your repo URL** (and optionally your public IP) in `config.env`:
   `REPO_URL` is where the control VM clones from; `AUTHOR_IP` restricts SSH to
   you (left empty, `provision.sh` looks it up).
5. **Run preflight and fix every FAIL:**
   ```
   ./deploy/gcp/preflight.sh --deployment sh7
   ```
   It checks gcloud, auth, project, billing, the Compute API, and whether the
   region's vCPU quota covers the deployment. `provision.sh` refuses to run while
   any check is red.

## Which deployments the free trial can run

| deployment | VMs | external IPs | fits the trial? |
|---|---|---|---|
| `sh1` | 2 (1 shard + control) | 2 | yes |
| `sh4` | 5 (4 shards + control) | 5 | yes |
| `sh7` | 8 (7 shards + control) | 8 | yes — exactly the cap |
| `sh8` | 9 (8 shards + control) | 9 | **no** — trial caps `IN_USE_ADDRESSES` at 8, and a trial cannot request an increase |

**`sh7` is the largest cluster the free trial can run.** `sh8` needs 9 external
IPs; use `sh7` instead, or upgrade to a paid account (keeps the credit) and raise
the quota. `sh4` runs the full OFAT matrix (43 cells for s1); `sh1` and `sh7` run
the 6-cell core (the OFAT extensions are anchored on `sh4`).

## Start a cluster and run every experiment on it

Do this once per deployment (`sh1`, `sh4`, or `sh7`). Each step is a command you
run from the repo root; `gcloud` must be on your PATH (it is in your normal
terminal after installing the SDK — if a step says `gcloud: command not found`,
run `export PATH="$HOME/google-cloud-sdk/bin:$PATH"` first).

```bash
DEP=sh7                                    # or sh1, sh4

# 1. Create the cluster (VPC is reused across deployments; ~1 min).
./deploy/gcp/provision.sh "$DEP"

# 2. Wait for the VMs to finish booting (install Docker, pull images, build the
#    runner). Takes ~2-4 min; this blocks until the control's router is up.
until gcloud compute ssh "mellow-${DEP}-control" --zone=us-central1-b --quiet \
        --command "docker ps --format '{{.Names}}'" 2>/dev/null | grep -q mongos; do
  echo "waiting for the cluster to boot..."; sleep 20
done

# 3. Initialise the cluster: register every shard at its private IP.
./deploy/gcp/init-cluster.sh "$DEP"

# 4. Run all five experiments. Results land in results-cloud/$DEP/<experiment>/.
for exp in s1 s2 s3 s4 s5; do
  ./deploy/gcp/run.sh "$DEP" --experiment "$exp" --profile smoke
done

# 5. Confirm what is up, then shut it down.
./deploy/gcp/status.sh
./deploy/gcp/teardown.sh "$DEP" --all      # deletes the VMs AND the VPC
```

To run all three deployments, repeat the block with `DEP=sh1`, then `sh4`, then
`sh7` (one cluster at a time — do not run them concurrently, or you exceed the
8-IP cap).

`run.sh` accepts `--experiment s1..s5` (default `s1`) and `--profile
smoke|small|medium|full` (default `smoke`). It sets `BENCH_ROUTER_URI` to the
control VM's router with a bounded timeout, runs the experiment in the runner
image on the control VM, tags each row with the git sha, and copies the CSVs back
to `results-cloud/<deployment>/`.

**`s2` (rebalancing) needs a bigger profile.** At `smoke` (1000 records) the
balancer has too little data to migrate, so `s2` reports `chunks_moved=0`. Run it
at `--profile medium` (100k) to see real migrations; the other four are meaningful
at `smoke`.

## Acceptance checklist (the validation milestone)

For each of `sh1`, `sh4`, `sh7` (see the start-a-cluster block above for the
boot-wait between provision and init):

1. `./deploy/gcp/provision.sh <dep>`
2. wait for boot, then `./deploy/gcp/init-cluster.sh <dep>`
3. `./deploy/gcp/run.sh <dep> --profile smoke`
4. Confirm a non-empty CSV with the série-S columns appears under `results-cloud/<dep>/`.
5. `./deploy/gcp/teardown.sh <dep> --all`

Then run one **timing probe** on `sh4` to record real per-cell time on the cloud:
`./deploy/gcp/run.sh sh4 --profile medium` (100k records). That number decides
whether the full multi-hour campaign is worth committing.

## Cost

VMs bill by wall-clock, not CPU-hours: eight VMs left up over a week of sessions
cost far more than the compute you use. So:

- `stop.sh <dep>` between sessions, `start.sh <dep>` to resume.
- `teardown.sh <dep> --all` when the campaign is done.
- `status.sh` to catch anything left running.
- Set a **billing alert (~US$50)** in the console; the whole validation costs a
  few dollars, well inside the US$300 trial credit.
