# Semantic Heterogeneous Database (MellowDB)

A prototype middleware for managing **semantic evolution** in databases, built on top of MongoDB.
Accompanies the paper *"Managing semantic evolution in databases: From theory to implementation"* (FGCS 2026).

## Overview

MellowDB lets you query a semantically heterogeneous data collection transparently — records
from different time periods that used different terminologies are automatically reconciled,
either at query time or at insertion time depending on the chosen strategy.

Two strategies are implemented and benchmarked:

| Strategy | Handles evolution at | Best for |
|----------|----------------------|----------|
| **Preprocess** | Insertion time (eager) | Read-heavy workloads |
| **Rewrite** | Query time (lazy) | Write-heavy workloads (≥ 95 % inserts) |

The same Python API serves a single MongoDB node, a replica set or a sharded cluster — only
the connection string changes, plus an optional shard key when sharding.

---

## Four ways to run

Pick the setup that matches what you want to do. All four run the exact same library; they
differ only in how MongoDB is provided.

| # | Way to run | MongoDB provided by | Best for |
|---|------------|---------------------|----------|
| **1** | [Local](#1-local) | A MongoDB you install/run yourself | Library development, quick local runs |
| **2** | [Single node via Docker Compose](#2-single-node-via-docker-compose) | One Docker container | Reproducible single-node benchmarks |
| **3** | [Replica set via Docker Compose](#3-replica-set-via-docker-compose-3-or-5-nodes) | 3 or 5 Docker containers | Distributed experiments, failover, consistency |
| **4** | [Sharded cluster via Docker Compose](#4-sharded-cluster-via-docker-compose-1-4-or-8-shards) | A config server, 1, 4 or 8 shards and a router, in Docker | Partitioning, shard keys |

All benchmark output goes to the git-ignored `results/` directory, so finished runs never
clutter the repository.

> Running the **experiments**? Skip straight to [Running benchmarks](#running-benchmarks).
> `./bench.sh` covers replication (B1/B3/B4/B6) and `./shard-bench.sh` covers sharding
> (S1–S5); both provision the deployments themselves and need nothing but Docker and bash.

---

## 1. Local

Run MellowDB directly on your machine against a MongoDB you manage yourself.

### Setup

```bash
# 1. Python 3.12+
python3 --version            # must be ≥ 3.12

# 2. uv (Python package manager)
curl -Ls https://astral.sh/uv/install.sh | sh

# 3. Project dependencies
uv sync

# 4. MongoDB 8.0 (Ubuntu example)
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/8.0 multiverse" | \
    sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt-get update && sudo apt-get install -y mongodb-org
sudo systemctl start mongod
```

### Run a benchmark

```bash
uv run benchmarks/simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=3 --evolution_fields=2 --operations=500 \
    --update_percent=0.05 --mode=preprocess --method=operations_first \
    --destination=results/local_preprocess.csv
```

`simulations.py` connects to `localhost:27017` by default. Override with `--host` or the
`MONGO_HOST` environment variable. Stop MongoDB when done with `sudo systemctl stop mongod`.

---

## 2. Single node via Docker Compose

A self-contained MongoDB container plus a Python "runner" container — nothing to install but Docker.

### Start MongoDB

```bash
docker compose up --build -d
docker compose ps          # wait until mongodb is "healthy"
```

This starts MongoDB 8.0 with a persistent volume (`mongodb_data`) and a health check.

### Run a benchmark

```bash
docker compose run --rm runner python benchmarks/simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=3 --evolution_fields=2 --operations=500 \
    --update_percent=0.05 --mode=preprocess --method=operations_first \
    --destination=results/docker_preprocess.csv
```

Inside the runner, `MONGO_HOST` is preset to `mongodb` (the Compose service name), so no
`--host` flag is needed. The repository is mounted at `/app`, so results land in your local
`results/` directory.

### Run the tests

```bash
docker compose run --rm runner python -m pytest -v
```

### Stop and clean up

```bash
docker compose down        # stop containers, keep the data volume
docker compose down -v     # stop containers and delete all data
```

---

## 3. Replica set via Docker Compose (3 or 5 nodes)

Runs a MongoDB replica set in Docker — required for distributed benchmarks, failover testing,
and the distributed test suite. Two topologies are provided:

| Topology | Compose file | Members (host ports) |
|----------|--------------|----------------------|
| 3 nodes | `docker-compose.replicaset.yml` | primary `27017`, secondaries `27018`, `27019` |
| 5 nodes | `docker-compose.replicaset5.yml` | primary `27017`, secondaries `27018`–`27021` |

### Start the replica set

```bash
# 3-node
docker compose -f docker-compose.replicaset.yml up -d

# 5-node
docker compose -f docker-compose.replicaset5.yml up -d
```

Each file includes a one-shot `mongo-init` container that runs `rs.initiate(...)` and exits
once a PRIMARY is elected. Wait ~30 seconds, then verify:

```bash
mongosh --port 27017 --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr}))"
```

Expected: one `PRIMARY`, the rest `SECONDARY`.

### Run a benchmark against the replica set

Drive it from the host with `uv` (works for both topologies — set `--nodes` to match):

```bash
uv run benchmarks/simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=30 --evolution_fields=2 --operations=500 \
    --update_percent=0.05 --mode=preprocess --method=operations_first \
    --mongo_uri="mongodb://localhost:27017/?directConnection=true" \
    --write_concern=majority --nodes=3 \
    --destination=results/rs3_preprocess.csv
```

- `--write_concern` accepts `1`, `majority` (default), or `all`.
- `--nodes` is metadata only — it is written to the output CSV so results from different
  topologies can be compared. Use `--nodes=5` for the 5-node set.

### Stop the replica set

```bash
docker compose -f docker-compose.replicaset.yml down       # keep data volumes
docker compose -f docker-compose.replicaset.yml down -v    # delete all data
# (use docker-compose.replicaset5.yml for the 5-node set)
```

---

## 4. Sharded cluster via Docker Compose (1, 4 or 8 shards)

Runs a sharded MongoDB cluster in Docker: one config server, one, four or eight shards and
one `mongos` router. MellowDB only ever talks to the router, so it is the only service that
publishes a host port.

| Topology | Compose file | Services |
|----------|--------------|----------|
| 1 shard | `docker-compose.shard1.yml` | `cfg1`, `shard1`, `mongos` (host port `27017`) |
| 4 shards | `docker-compose.shard4.yml` | `cfg1`, `shard1`–`shard4`, `mongos` (host port `27017`) |
| 8 shards | `docker-compose.shard8.yml` | `cfg1`, `shard1`–`shard8`, `mongos` (host port `27017`) |

**`sh1` exists to be measured against.** It is the same code path as the other two — a
router, a config server, sharded collections — with nothing to distribute across, so it is
the intercept the scale axis is read from. Comparing `sh4` against a plain standalone would
change the router, the config server and the shard count all at once, and no single number
could be attributed to distribution.

Each shard is a single-node replica set. That is deliberate: this deployment is for questions
about partitioning, not replication, which the replica-set stacks above already cover. It
also means there is no secondary to offload reads to, so `read_uri` / `read_mode` do not
apply here.

### Start the cluster

```bash
# 4 shards (use docker-compose.shard8.yml for 8)
docker compose -f docker-compose.shard4.yml up -d
docker compose -f docker-compose.shard4.yml wait mongo-init   # exits 0 once the cluster is ready
```

The one-shot `mongo-init` container initiates the config server and every shard, then
registers the shards with the router. It is safe to re-run. Verify:

```bash
docker compose -f docker-compose.shard4.yml logs --no-log-prefix mongo-init | tail -1
docker compose -f docker-compose.shard4.yml exec -T mongos \
    mongosh --quiet --eval 'db.getSiblingDB("config").shards.countDocuments({})'
```

Expected: `Sharded cluster ready with 4 shards.` and `4`.

Two environment variables adjust the stack without editing it:

| Variable | Default | Effect |
|----------|---------|--------|
| `MONGOS_HOST_PORT` | `27017` | Host port the router is published on |
| `MONGO_CACHE_GB` | `0.25` | WiredTiger cache per `mongod` — the minimum, so 8 shards fit a laptop; raise it on a benchmark machine |

Every stack in this repository publishes on `27017` by default, so run one at a time or move
the router with `MONGOS_HOST_PORT`. If you adapt these files for another machine, keep the
`nofile` ulimit: without it a shard runs out of file descriptors under load and aborts.

### Stop the cluster

```bash
docker compose -f docker-compose.shard4.yml down       # keep data volumes
docker compose -f docker-compose.shard4.yml down -v    # delete all data
```

---

## Running benchmarks

The distributed extension raises questions the original single-node benchmarks could not
ask — does offloading reads to secondaries actually buy throughput, and does replication
change which evolution strategy wins? `./bench.sh` answers them reproducibly.

**Your machine needs only Docker and bash.** MongoDB, the MellowDB library and every
experiment run inside containers; `bench.sh` just sequences them.

```bash
./bench.sh b1                       # profile=small, the experiment's default deployments
./bench.sh b1 --profile smoke       # ~1 minute sanity run
./bench.sh all --profile full       # the real result run
```

For each deployment in the sweep it runs: **compose up → wait for a healthy primary →
run the experiment in an in-network runner container → write the CSV → `compose down -v`**.

> **`bench.sh` is destructive by design.** Every cell ends with `docker compose down -v`,
> which deletes that deployment's volumes so the next cell starts from clean data. Do not
> point it at a deployment holding data you care about.

### The experiments

| ID | Question it answers | Default deployments |
|----|---------------------|---------------------|
| `b1` | Does routing record reads to secondaries raise aggregate query throughput? Workers are assigned nodes round-robin, so reads genuinely fan out. | `single,rs3,rs5` |
| `b3` | What does durability cost as the cluster grows? Insert-only, swept across write concerns `1 / majority / all`. | `single,rs3,rs5` |
| `b4` | Does distribution change which strategy wins? Mixed read/write across `preprocess` vs `rewrite`. | `single,rs3,rs5` |
| `b6` | How does query cost grow as semantic operations stack up (chain length 1→50)? | `single,rs3` |

`./bench.sh all` runs `b1`, `b3`, `b6`, then `b4` (largest matrix last).

> `bench.sh` refuses `sh1` / `sh4` / `sh8`, and the B-series experiments refuse them too:
> their central axes — `read_target`, `read_mode`, `write_concern` — mean nothing on
> single-node shards. Sharding has its own entrypoint,
> [`./shard-bench.sh`](#sharding-benchmarks-shard-benchsh).

### Options

| Option | Default | Meaning |
|--------|---------|---------|
| `--profile <smoke\|small\|medium\|full>` | `medium` | Sizing profile (below) |
| `--deployments <csv>` | per experiment | Override the sweep, e.g. `single,rs3` |
| `--corpus <synthetic\|real>` | `synthetic` | Data source |
| `--out <dir>` | `results` | Output directory — **must be repo-relative** (below) |
| `--keep-going` | off | Continue the sweep after a failing cell |
| `--fresh` | off | Re-run cells that already have rows |

> **`--out` is read inside the runner container**, where the repository is mounted at
> `/app`. An absolute host path would be written inside the container and lost when it
> exits, taking the campaign's resume state with it — so an absolute path is refused
> outright rather than silently discarding results.

**Campaigns are resumable.** Every row is written the moment it is measured, and a cell
that already has rows is skipped, so an interrupted run keeps everything it finished and
re-running continues where it stopped. The scan covers every CSV in the experiment's
directory, not just today's, so a campaign split across days resumes correctly. `--fresh`
runs everything again.

### Profiles

Sizes are fixed data, never derived from the machine, so numbers stay comparable across
machines. The default is deliberately small enough to finish on a modest laptop.

| Profile | Records | Clients | Warmup | Window | Reps | Real-corpus files | Rough runtime |
|---------|---------|---------|--------|--------|------|-------------------|---------------|
| `smoke` | 1 000 | 2 | 2 s | 5 s | 1 | 1 | ~1 min |
| `small` | 20 000 | 4 | 5 s | 20 s | 3 | 3 | ~10 min |
| `medium` (default) | 100 000 | 6 | 10 s | 40 s | 3 | 10 | hours |
| `full` | 200 000 | 8 | 10 s | 60 s | 5 | all | many hours |

`records` sizes the synthetic corpus. The real corpus is sized instead by how many of the
43 yearly DATASUS CSVs to load, since its record count is fixed by the data.

### Synthetic vs real data

The default corpus is **synthetic** — `DatabaseGenerator` fabricates records and semantic
operations at runtime, so a fresh clone benchmarks with zero setup.

`--corpus real` uses the DATASUS mortality dataset at
`dataset/MellowDB_experiments/` (source CSVs plus the CID-9 → CID-10 operations). That
directory is git-ignored; if it is missing, the run **fails immediately naming the expected
path** rather than silently falling back to synthetic and mislabelling the results.

> Real-corpus runs are far slower: loading a single yearly file plus its 170 semantic
> operations takes roughly 3 minutes, and the corpus is rebuilt per cell.

### Results

Every experiment appends rows to `results/<experiment>/<experiment>_<profile>_<date>.csv`
(git-ignored). **The four B-series experiments share one wide schema**, so their CSVs
concatenate and filter cleanly in pandas. The S series has
[a schema of its own](#reading-the-results); do not concatenate the two.

```python
import pandas as pd, glob
df = pd.concat([pd.read_csv(f) for f in glob.glob("results/*/*.csv")], ignore_index=True)
```

Columns worth knowing:

| Column | Meaning |
|--------|---------|
| `read_target` | `primary`, `secondaries` (split mode), or `secondaries_single_source` |
| `read_mode` | `split` = version chain from primary, records from the read node; `single_source` = both from the read node |
| `mix` | `read_only`, `write_only`, `read_heavy` (95 % reads), `write_heavy` (5 % reads) |
| `chain_length` | Number of stacked semantic operations in the corpus |
| `setup_insert_s` / `setup_operations_s` | Corpus build cost — where `preprocess` pays and `rewrite` does not |
| `error_rate` | Failed attempts / all attempts. Any cell above 1 % also prints a warning; treat those rows with suspicion |
| `git_sha`, `mongo_version`, `host_cpus`, `host_mem_gb` | Provenance, so results from different machines are never silently compared |

Columns that do not apply to an experiment are left empty. Plotting and analysis are done
separately — nothing in this repository reads the CSVs back.

---

## Sharding benchmarks (`shard-bench.sh`)

Sharding gets an entrypoint of its own. It shares the neutral machinery with `bench.sh` —
profiles, metrics, the load driver, the corpus builders — and nothing else: the B series is
built around `read_target`, `read_mode` and `write_concern`, and all three are meaningless
on single-node shards.

```bash
./shard-bench.sh s1                       # profile=medium, deployments sh1,sh4,sh8
./shard-bench.sh s3 --profile smoke        # a few minutes, proves the pipeline
./shard-bench.sh all --profile medium      # s1, s3, s4, s5
```

### The experiments

| ID | Question it answers | Kind |
|----|---------------------|------|
| `s1` | What does applying one semantic operation cost, and how much data does it move between shards? | one-shot event |
| `s2` | Once the evolution has unbalanced the cluster, what does it cost to put it back? | one-shot event, balancer on |
| `s3` | What is naming the shard key in a filter worth, and what does it cost not to? | steady state |
| `s4` | Does the rewrite's advantage over preprocess grow with the shard count? | steady state, mixed load |
| `s5` | What does MellowDB's schemaless promise cost on a cluster? | one-shot event |

`s2` has no sweep of its own: it rides along on `s1`'s core cells, because building a corpus
is 99% of a cell's cost and `s2` measures a second thing about the state `s1` just created.
`./shard-bench.sh all` therefore runs `s1 s3 s4 s5`, and `s2`'s rows appear anyway.

### The shard key matrix

S1 and S3 sweep six shard keys. The axis that matters is **how the key relates to semantic
evolution**, not whether it is hashed or ranged:

| | hashed | ranged |
|---|--------|--------|
| **`_id`** — a system field | control: copies scatter at random, nothing unbalances | declared negative control: ObjectId only grows, so everything lands in the last chunk |
| **a data field** the operations never touch | the realistic production choice | the realistic choice, plus whatever skew the data has |
| **the field that evolves** | the copies move | the copies move, and skew concentrates them |

`_id` is the control the other two rows are read against: `split_processed_records` unsets
`_id` before the `$merge`, so every copy is born with a fresh one and lands wherever it
hashes.

### Options beyond `bench.sh`'s

| Option | Default | Meaning |
|--------|---------|---------|
| `--chunk-size-mb <n>` | `1` | Cluster-wide chunk size |

**1 MB is an experimental condition, not a production default.** MongoDB's built-in 128 MB
makes the balancer inert at any corpus size this campaign can afford — a measured run of
four chunks holding 956 KB produced zero migrations — so `s2` would report a cluster that
never needed rebalancing. Say so in anything published from these numbers.

### Reading the results

The S series writes **its own CSV schema**, disjoint from the B series': one wide schema
covering both would leave every row of each half empty.

| Column | Meaning |
|--------|---------|
| `shards` | 1, 4 or 8 |
| `shard_key_role` / `shard_key_kind` | `id` / `data` / `evolved`, and `hashed` / `ranged` |
| `skew` | Zipf exponent of the synthetic corpus. Also the coverage axis: at a 20-value domain the most frequent value covers 6% at skew 0, 46% at 1.5 and 83% at 3.0 — the range DATASUS's `cid` sits in |
| `dist_before` / `dist_after` | Documents per shard as JSON, every shard listed. The raw datum the rest derives from |
| `imbalance_before` / `imbalance_after` | Heaviest shard over a fair share. 1.0 is perfect, and the shard count is the worst possible |
| `docs_relocated` | Copies that landed on a shard other than the one their original sits on |
| `jumbo_chunks` | Chunks too large for MongoDB to move |
| `chunks_moved` / `bytes_moved` / `converge_s` | What rebalancing cost (`s2`) |
| `shards_touched` | How many shards a query reached, from the query planner (`s3`) |

Two things that will trip up the analysis:

- **There is no `coverage` column.** A uniform corpus cannot honour a requested coverage —
  every value covers about 1/cardinality of it — so asking for 80% of a 20-value domain
  touches 6%. What an operation really touched is `docs_written / docs_before`.
- **`skew` collides with `DataFrame.skew()`.** `f.skew == 3.0` compares a method and
  silently matches nothing. Use `f['skew']`.

### What the numbers cannot say yet

- **`smoke` cannot measure `s2`.** A 1000-record corpus is around 350 KB, below the 1 MB
  chunk size, so there is nothing to rebalance and every cell reports zero migrations. It
  proves the pipeline, not the measurement.
- **`docs_relocated` is a coin flip per cell, not a rate.** A translation sends every copy
  to the one shard its new value routes to, so a single repetition says only whether that
  shard happened to be the origin. Each repetition draws a different corpus; read the
  repetitions together.

---

## Running tests

MellowDB has two test suites:

- **Unit tests** (`semantic_heterogeneous_database/tests/`) need only a running MongoDB —
  single node, replica set or sharded cluster. Topology is detected automatically.
- **Distributed tests** (`tests/distributed/`) additionally require a running replica set.

A bare `uv run pytest` collects three directories — the library, the CLI and the benchmark
harness — including `slow` tests that bring Docker stacks up. Pass a path, or `-m "not slow"`,
to run less.

```bash
# Library unit tests
uv run pytest semantic_heterogeneous_database/tests -v

# Library, CLI and harness, without the slow Docker-driven tests
uv run pytest -m "not slow" -v

# Distributed tests — start a replica set first (way 3 above)
uv run pytest tests/distributed/ -m "not slow" -v          # fast: correctness + concurrency
uv run pytest tests/distributed/ -m slow -v --timeout=180  # slow: failover + consistency rate

# Everything (unit + fast distributed)
uv run pytest semantic_heterogeneous_database/tests/ tests/distributed/ -m "not slow" -v
```

### Running the tests without host Python

The same suites run inside the runner container, so a machine with only Docker can execute
them. Start a replica set first (way 3 above), then:

```bash
docker compose -f docker-compose.replicaset.yml run --rm \
  --user "$(id -u):$(id -g)" -e HOME=/tmp \
  -e MONGO_HOST="mongodb://mongo-primary:27017/?directConnection=true" \
  runner python -m pytest benchmarks/tests -m "not slow" -q
```

`--user` keeps any files the run creates owned by you rather than root. `MONGO_HOST` points
at the Compose service hostname, which resolves inside the network but not from the host.

### What the distributed suite covers

| File | Marks | What it tests |
|------|-------|---------------|
| `test_distributed_correctness.py` | (none) | Semantic correctness of translation, grouping, rewrite mode on a replica set; verifies version reads route to PRIMARY with majority concern |
| `test_concurrent_operations.py` | (none) | Concurrent `execute_operation` calls leave the version chain consistent — no duplicate version numbers, no broken pointers |
| `test_failover.py` | `slow` | Primary failure and election: data written before failover survives; new operations work after election |
| `test_semantic_consistency_rate.py` | `slow` | Quantifies semantic correctness (F1) under combinations of write concern and version read concern; majority reads must yield 100 % F1 |

### Running the tests against a sharded cluster

Start a sharded cluster first (way 4 above). Two environment variables decide what runs:

| Variable | Effect |
|----------|--------|
| `MELLOW_SHARD_KEY` | JSON shard key applied to every collection the suite builds, so the whole semantic suite runs sharded |
| `MELLOW_SHARDED=1` | Enables the tests that only make sense on a cluster (placement, shard key lifecycle); they are skipped otherwise |

```bash
# The whole library suite, with every collection sharded on _id
MONGO_HOST=mongodb://localhost:27017 MELLOW_SHARD_KEY='{"_id": "hashed"}' \
    uv run pytest semantic_heterogeneous_database/tests -v

# The cluster-only tests: the library's, and the sharding harness's
MELLOW_SHARDED=1 MONGO_HOST=mongodb://localhost:27017 \
    uv run pytest semantic_heterogeneous_database/tests/test_sharding.py \
                  semantic_heterogeneous_database/tests/test_split_materialization.py \
                  semantic_heterogeneous_database/tests/test_sharded_placement.py \
                  benchmarks/tests/test_sharding_metrics.py \
                  benchmarks/tests/test_cell_setup.py \
                  benchmarks/tests/test_experiment_s5.py -v
```

The Compose stacks themselves are tested by bringing each one up under its own project name
and port, so a cluster you already have running is never touched:

```bash
uv run pytest benchmarks/tests/test_sharded_init.py -m slow -v
```

---

## Operator CLI (`mellow_cli`)

An interactive shell + one-shot subcommands to operate a real database. Run from the repo root:

```bash
# Start a 3-node replica set and drop into the shell
uv run python -m mellow_cli up --deployment rs3 --db mortality --mode preprocess

# In the shell — load the DATASUS data, apply the CID-9→CID-10 evolutions, query
mellow> load dataset/MellowDB_experiments/source_data RefDate
mellow> operations dataset/MellowDB_experiments/semantic_operations/operations_cid9_cid10.csv
mellow> query {'cid': '104 Acidentes de transporte'}
mellow> read-node 27018          # offload record reads to a secondary (split mode)
mellow> status
mellow> exit                     # data + containers preserved
```

One-shot equivalents (each connects, runs, exits):

```bash
uv run python -m mellow_cli connect --deployment rs3 --db mortality   # reattach to the shell
uv run python -m mellow_cli query --deployment rs3 --db mortality "{'cid': '104 Acidentes de transporte'}"
uv run python -m mellow_cli destroy --deployment rs3 --db mortality --yes   # drop DB + docker compose down -v
```

| Command | Effect |
|---------|--------|
| `up --deployment single\|rs3\|rs5\|sh1\|sh4\|sh8` | compose up + wait + connect (on the sharded stacks, waits until every shard is registered) |
| `connect` / `shell` | attach to a running deployment (no Docker touch) |
| `load <folder> [date_field]` | bulk-load a folder of CSVs (REPL); one-shot: `load <folder> --date-field RefDate` |
| `operations <file>` | apply a `;`-delimited operations CSV |
| `query '<dict>'` / `count '<dict>'` | query (dict or single-quoted dict syntax) |
| `read-node <port\|primary> [split\|single_source]` | switch the record-read node live (replica sets only) |
| `drop --yes` | drop the database, keep Docker running |
| `destroy --yes` | drop the database **and** `docker compose down -v` |

On `sh1` / `sh4` / `sh8` the CLI connects to the router on port `27017` (it does not read
`MONGOS_HOST_PORT`) and has no shard key option: collections it creates are not sharded and
live whole on the database's primary shard. A collection already sharded through the library
is discovered and used as is.

---

## Simulation arguments

| Argument | Type | Description |
|----------|------|-------------|
| `--records` | int | Number of records to generate |
| `--versions` | int | Number of semantic evolution operations to apply |
| `--fields` | int | Number of attributes per record |
| `--domain` | int | Number of distinct values per attribute |
| `--evolution_fields` | int | Number of attributes that will undergo semantic evolution |
| `--operations` | int | Number of benchmark operations (inserts + queries) |
| `--update_percent` | float [0–1] | Fraction of operations that are inserts (0 = read-only, 1 = write-only) |
| `--method` | string | Initialization strategy: `operations_first` or `insertion_first` |
| `--mode` | string | Semantic evolution strategy: `preprocess` or `rewrite` |
| `--repetitions` | int | Number of times to repeat the test |
| `--destination` | string | Output CSV file path (use `results/…` to keep it git-ignored) |
| `--host` | string | MongoDB host (overrides `MONGO_HOST`; default `localhost`) |
| `--mongo_uri` | string | Full MongoDB URI — use for replica sets |
| `--write_concern` | string | Write concern: `1`, `majority` (default), or `all` |
| `--nodes` | int | Replica-set member count — written to the CSV as metadata |
| `--read_uri` | string | Direct URI of the node to read records from (e.g. a secondary). Default: read from the primary |
| `--read_mode` | string | `split` (default): version chain from primary, records from `--read_uri`; `single_source`: everything from `--read_uri` |

### Initialization strategies

| Method | Description |
|--------|-------------|
| `operations_first` | Register all semantic evolutions first, then insert records. Faster overall. |
| `insertion_first` | Insert records first, then register evolutions (triggers full reprocessing). Benefits from indexes. |

For the full list:

```bash
uv run benchmarks/simulations.py --help
```

---

## Project structure

```
semantic_heterogeneous_database/   # MellowDB library
  BasicCollection.py               #   public API (mongo_uri + write_concern)
  Collection.py                    #   core engine: insert, find, query rewriting, write concern
  TranslationOperation.py          #   1-to-1 rename
  GroupingOperation.py             #   many-to-1 merge
  UngroupingOperation.py           #   1-to-many split
  SemanticOperation.py             #   abstract base class
  sharding.py                      #   shard key, mongos detection, idempotent shardCollection, placement
  tests/                           #   unit tests (any MongoDB topology)

bench.sh                           # replication benchmarks — needs only Docker + bash
shard-bench.sh                     # sharding benchmarks — same requirements

benchmarks/                        # research scripts that use the library
  harness/                         #   the benchmark harness
    profiles.py                    #     smoke/small/medium/full sizing
    topology.py                    #     deployment → compose file + in-network node URIs
    corpus.py                      #     synthetic and real DATASUS corpora
    workload.py                    #     concurrent load driver (warmup + fixed window)
    metrics.py                     #     throughput and latency percentiles
    results.py                     #     the two CSV schemas and the writer
    resume.py                      #     which cells of a campaign already ran
    runner.py                      #     B-series plumbing (read targets, write concern)
    shard_runner.py                #     S-series plumbing (shard keys, chunk size)
    sharding_metrics.py            #     distribution, relocation, jumbo chunks, balancer
    cell_setup.py                  #     shard key matrix, pre-split, pre-flight guard
  experiments/                     #   one module per experiment
    b1_read_offloading.py          #     B1 — read offloading throughput
    b3_write_replication.py        #     B3 — write cost of replication
    b4_strategy_distribution.py    #     B4 — strategy × distribution
    b6_chain_depth.py              #     B6 — version-chain depth
    s1_operation_cost.py           #     S1 — cost of one semantic operation
    s2_rebalance.py                #     S2 — cost of rebalancing afterwards
    s3_targeting.py                #     S3 — targeted queries vs broadcast
    s4_crossover_scale.py          #     S4 — strategy crossover across scale
    s5_schemaless.py               #     S5 — the price of being schemaless
  simulations.py                   #   older single-node benchmark CLI
  database_generator.py            #   synthetic records + operations (fields evo0.., f0..)
  bench_utils.py                   #   write-concern helpers
  tests/                           #   harness and experiment tests
  legacy/                          #   older experiments, pending rework
    simulations_realcases.py       #     benchmark over the real DATASUS dataset
    simulations_realcases_operations.py
    simulations_writer.py          #     prints batch command combinations

tests/distributed/                 # tests that require a replica set
docker/
  runner/                          #   Python 3.12 runner image
  replicaset/                      #   rs.initiate() scripts (3- and 5-node, idempotent)
  sharded/                         #   config server + shard init and addShard (idempotent)
analysis/                          # scripts that generate the paper figures

docker-compose.yml                 # way 2 — single node
docker-compose.replicaset.yml      # way 3 — 3-node replica set
docker-compose.replicaset5.yml     # way 3 — 5-node replica set
docker-compose.shard1.yml          # way 4 — 1-shard cluster (the scale intercept)
docker-compose.shard4.yml          # way 4 — 4-shard cluster
docker-compose.shard8.yml          # way 4 — 8-shard cluster
pyproject.toml                     # dependencies and pytest configuration
```

> The benchmark scripts are run in place (the library is not pip-installed), so each one adds
> the repository root to `sys.path` before importing `semantic_heterogeneous_database`.

---

## Using MellowDB as a library

```python
from semantic_heterogeneous_database import BasicCollection
from datetime import datetime

# Single node (default)
col = BasicCollection('mydb', 'mycollection', operation_mode='preprocess')

# Replica set
col = BasicCollection(
    'mydb', 'mycollection',
    mongo_uri='mongodb://localhost:27017/?directConnection=true',
    operation_mode='preprocess',
    write_concern='majority',
)

# Insert records with their validity date
col.insert_one('{"city": "Piçarras", "population": 50000}', datetime(2000, 1, 1))

# Register a semantic evolution: "Piçarras" was renamed in 2004
col.execute_operation('translation', datetime(2004, 1, 1), {
    'fieldName': 'city',
    'oldValue': 'Piçarras',
    'newValue': 'Balneário Piçarras',
})

# Load many evolution operations from a CSV file
col.execute_many_operations_by_csv('operations.csv', 'type', 'valid_from')

# Query transparently — returns records from all periods under the current name
results = col.find_many({'city': 'Balneário Piçarras'})
col.pretty_print(results)

# Create indexes for better query performance
col.create_index(['city'])
```

### Reading from a specific node (replica sets)

On a replica set you can offload the heavy *record* reads to a chosen node while the
*version chain* (which must stay fresh for correct translation) keeps reading from the
primary. Writes always go to the primary — MongoDB enforces this.

```python
# read_mode='split' (default):
#   writes        -> primary    (localhost:27017)
#   version chain -> primary    (localhost:27017)
#   record data   -> secondary  (localhost:27018)
col = BasicCollection(
    'mydb', 'mycollection',
    mongo_uri='mongodb://localhost:27017/?directConnection=true',   # primary   -> writes + version chain
    read_uri='mongodb://localhost:27018/?directConnection=true',    # secondary -> record data
    read_mode='split',
)

# read_mode='single_source' (read everything from one node; accepts staleness on purpose):
#   writes        -> primary    (localhost:27017)
#   version chain -> secondary  (localhost:27019)
#   record data   -> secondary  (localhost:27019)
col = BasicCollection(
    'mydb', 'mycollection',
    mongo_uri='mongodb://localhost:27017/?directConnection=true',   # primary   -> writes
    read_uri='mongodb://localhost:27019/?directConnection=true',    # secondary -> version chain + record data
    read_mode='single_source',
)

# Switch the read node / mode at runtime (e.g. point reads at a different secondary)
col.set_read_source('mongodb://localhost:27019/?directConnection=true', read_mode='split')
col.set_read_source(None)   # back to reading everything from the primary
```

### Sharded clusters

Connect through the router and pass a `shard_key` — MongoDB's own key pattern, the same
dictionary `shardCollection` takes:

```python
col = BasicCollection(
    'mydb', 'mycollection',
    mongo_uri='mongodb://localhost:27017',   # the mongos router
    operation_mode='preprocess',
    shard_key={'city': 'hashed'},            # or {'city': 1}, {'_id': 'hashed'}
)
```

- **What gets sharded:** in `preprocess` mode, the raw collection and
  `<collection>_processed`; in `rewrite` mode, the raw collection. The version chain and the
  columns metadata are never sharded — they are small and stay whole on the database's
  primary shard.
- **Idempotent:** reopening a collection with the same key does nothing, and a different key
  raises `MellowDBError` instead of re-sharding. With no `shard_key`, an existing shard key is
  discovered and used.
- **Off a cluster:** against a single node or a replica set the key is ignored with a
  `RuntimeWarning`, so the same code runs everywhere.
- **Schemaless:** a document may lack the shard key field. It is stored, and `preprocess`
  still evolves it; its evolved copies are written back by the driver instead of `$merge`,
  so more slowly.
- **Ranged keys and small data:** a ranged key starts as a single chunk, so a small
  collection stays on one shard; a hashed key spreads from the start.

To see where the data actually is:

```python
from semantic_heterogeneous_database import sharding

sharding.distribution(col.collection.db, 'mycollection_processed')
# e.g. {'shard1': 122, 'shard2': 133, 'shard3': 138, 'shard4': 74}
```
