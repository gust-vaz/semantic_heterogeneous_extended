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

### What the distributed suite covers

| File | Marks | What it tests |
|------|-------|---------------|
| `test_distributed_correctness.py` | (none) | Semantic correctness of translation, grouping, rewrite mode on a replica set; verifies version reads route to PRIMARY with majority concern |
| `test_concurrent_operations.py` | (none) | Concurrent `execute_operation` calls leave the version chain consistent — no duplicate version numbers, no broken pointers |
| `test_failover.py` | `slow` | Primary failure and election: data written before failover survives; new operations work after election |
| `test_semantic_consistency_rate.py` | `slow` | Quantifies semantic correctness (F1) under combinations of write concern and version read concern; majority reads must yield 100 % F1 |

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

benchmarks/                        # research scripts that use the library
  simulations.py                   #   main benchmark CLI
  database_generator.py            #   synthetic record + operation generator
  bench_utils.py                   #   write-concern helpers
  legacy/                          #   older experiments, pending rework
    simulations_realcases.py       #     benchmark over the real DATASUS dataset
    simulations_realcases_operations.py
    simulations_writer.py          #     prints batch command combinations

tests/distributed/                 # tests that require a replica set
docker/
  runner/                          #   Python 3.12 runner image
  replicaset/                      #   rs.initiate() scripts (3- and 5-node)
analysis/                          # R scripts that generate the paper figures

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
