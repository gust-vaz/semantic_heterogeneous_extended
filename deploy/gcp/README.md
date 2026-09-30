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
   ./deploy/gcp/preflight.sh --deployment sh8
   ```
   It checks gcloud, auth, project, billing, the Compute API, and whether the
   region's vCPU quota covers the deployment (sh8 needs 20). `provision.sh`
   refuses to run while any check is red.

## Run a deployment

```bash
./deploy/gcp/provision.sh    sh4          # VPC + 4 shard VMs + 1 control VM
./deploy/gcp/init-cluster.sh sh4          # register shards at their private IPs
./deploy/gcp/run.sh          sh4 --profile smoke
./deploy/gcp/status.sh                    # confirm what is up
./deploy/gcp/stop.sh         sh4          # between sessions (a stopped VM bills no CPU)
./deploy/gcp/start.sh        sh4          # resume next session
./deploy/gcp/teardown.sh     sh4          # delete the VMs when done (--all also removes the VPC)
```

`run.sh` accepts `--experiment s1..s5` (default `s1`) and `--profile
smoke|small|medium|full` (default `smoke`), sets `BENCH_ROUTER_URI` to the control
VM's router with a bounded selection timeout, runs the experiment module in the
runner image on the control VM, and copies the CSVs back to `./results-cloud/`.

## Acceptance checklist (the validation milestone)

For each of `sh1`, `sh4`, `sh8`:

1. `./deploy/gcp/provision.sh <dep>`
2. `./deploy/gcp/init-cluster.sh <dep>`
3. `./deploy/gcp/run.sh <dep> --profile smoke`
4. Confirm a non-empty CSV with the série-S columns appears under `./results-cloud/`.
5. `./deploy/gcp/teardown.sh <dep>`

Then run one **timing probe** on `sh4` to record real per-cell time on the cloud:
`./deploy/gcp/run.sh sh4 --profile medium` (100k records). That number decides
whether the full multi-hour campaign is worth committing.

## Cost

VMs bill by wall-clock, not CPU-hours: nine VMs left up over a week of sessions
cost far more than the compute you use. So:

- `stop.sh <dep>` between sessions, `start.sh <dep>` to resume.
- `teardown.sh <dep> --all` when the campaign is done.
- `status.sh` to catch anything left running.
- Set a **billing alert (~US$50)** in the console; the whole validation costs a
  few dollars, well inside the US$300 trial credit.
