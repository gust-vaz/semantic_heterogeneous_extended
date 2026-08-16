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

The same Python API serves a single MongoDB node or a replica set — only the connection
string changes.

---

## Three ways to run

Pick the setup that matches what you want to do. All three run the exact same library and
benchmarks; they differ only in how MongoDB is provided.

| # | Way to run | MongoDB provided by | Best for |
|---|------------|---------------------|----------|
| **1** | [Local](#1-local) | A MongoDB you install/run yourself | Library development, quick local runs |
| **2** | [Single node via Docker Compose](#2-single-node-via-docker-compose) | One Docker container | Reproducible single-node benchmarks |
| **3** | [Replica set via Docker Compose](#3-replica-set-via-docker-compose-3-or-5-nodes) | 3 or 5 Docker containers | Distributed experiments, failover, consistency |

All benchmark output goes to the git-ignored `results/` directory, so finished runs never
clutter the repository.

> Running the **distributed experiments** (B1/B3/B4/B6)? Skip straight to
> [Running benchmarks](#running-benchmarks) — `./bench.sh` provisions the deployments
> itself and needs nothing but Docker and bash.

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

### Options

| Option | Default | Meaning |
|--------|---------|---------|
| `--profile <smoke\|small\|full>` | `small` | Sizing profile (below) |
| `--deployments <csv>` | per experiment | Override the sweep, e.g. `single,rs3` |
| `--corpus <synthetic\|real>` | `synthetic` | Data source |
| `--out <dir>` | `results` | Output directory |
| `--keep-going` | off | Continue the sweep after a failing cell |

### Profiles

Sizes are fixed data, never derived from the machine, so numbers stay comparable across
machines. The default is deliberately small enough to finish on a modest laptop.

| Profile | Records | Clients | Warmup | Window | Reps | Real-corpus files | Rough runtime |
|---------|---------|---------|--------|--------|------|-------------------|---------------|
| `smoke` | 1 000 | 2 | 2 s | 5 s | 1 | 1 | ~1 min |
| `small` (default) | 20 000 | 4 | 5 s | 20 s | 3 | 3 | ~10 min |
| `full` | 200 000 | 8 | 10 s | 60 s | 5 | all | hours |

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
(git-ignored). **All four share one wide schema**, so the CSVs concatenate and filter
cleanly in pandas:

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

## Running tests

MellowDB has two test suites:

- **Unit tests** (`semantic_heterogeneous_database/tests/`) need only a running MongoDB —
  single node or replica set. Topology is detected automatically.
- **Distributed tests** (`tests/distributed/`) additionally require a running replica set.

```bash
# Unit tests (default pytest path)
uv run pytest -v

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
| `up --deployment single\|rs3\|rs5` | compose up + wait + connect |
| `connect` / `shell` | attach to a running deployment (no Docker touch) |
| `load <folder> [date_field]` | bulk-load a folder of CSVs (REPL); one-shot: `load <folder> --date-field RefDate` |
| `operations <file>` | apply a `;`-delimited operations CSV |
| `query '<dict>'` / `count '<dict>'` | query (dict or single-quoted dict syntax) |
| `read-node <port\|primary> [split\|single_source]` | switch the record-read node live |
| `drop --yes` | drop the database, keep Docker running |
| `destroy --yes` | drop the database **and** `docker compose down -v` |

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
  tests/                           #   unit tests (any MongoDB topology)

bench.sh                           # benchmark entrypoint — needs only Docker + bash

benchmarks/                        # research scripts that use the library
  harness/                         #   the benchmark harness
    profiles.py                    #     smoke/small/full sizing
    topology.py                    #     deployment → compose file + in-network node URIs
    corpus.py                      #     synthetic and real DATASUS corpora
    workload.py                    #     concurrent load driver (warmup + fixed window)
    metrics.py                     #     throughput and latency percentiles
    results.py                     #     shared wide CSV schema and writer
    runner.py                      #     shared experiment plumbing
  experiments/                     #   one module per experiment
    b1_read_offloading.py          #     B1 — read offloading throughput
    b3_write_replication.py        #     B3 — write cost of replication
    b4_strategy_distribution.py    #     B4 — strategy × distribution
    b6_chain_depth.py              #     B6 — version-chain depth
  simulations.py                   #   older single-node benchmark CLI
  database_generator.py            #   synthetic record + operation generator
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
analysis/                          # scripts that generate the paper figures

docker-compose.yml                 # way 2 — single node
docker-compose.replicaset.yml      # way 3 — 3-node replica set
docker-compose.replicaset5.yml     # way 3 — 5-node replica set
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
