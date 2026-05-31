# Semantic Heterogeneous Database (MellowDB)

A prototype middleware for managing **semantic evolution** in databases, built on top of MongoDB.
Accompanies the paper *"Managing semantic evolution in databases: From theory to implementation"* (FGCS 2026).

## Overview

MellowDB lets you query a semantically heterogeneous data collection transparently — records from different time periods that used different terminologies are automatically reconciled at query time or at insertion time, depending on the chosen strategy.

Two strategies are implemented and benchmarked:

| Strategy | Handles evolution at | Best for |
|----------|----------------------|----------|
| **Preprocess** | Insertion time (eager) | Read-heavy workloads |
| **Rewrite** | Query time (lazy) | Write-heavy workloads (≥ 95 % inserts) |

---

## Deployment Modes

MellowDB supports two MongoDB deployment modes:

| Mode | Setup | When to use |
|------|-------|-------------|
| **Single node** | `docker-compose.yml` | Development, unit tests, paper reproduction |
| **Replica set (3 nodes)** | `docker-compose.replicaset.yml` | Distributed experiments, availability testing |

Both modes share the same Python API — changing `mongo_uri` is all that is required at the application level.

---

## Getting Started

### Prerequisites

Choose **one** of the two setups below.

| | Docker | Local |
|---|--------|-------|
| Requirements | Docker, Docker Compose | Python 3.12+, MongoDB 8.0, `uv` |
| MongoDB managed by | Docker | You |
| Recommended for | Running experiments quickly | Library development |

---

## Option A — Docker (recommended for running simulations)

### 1. Start MongoDB

```bash
docker compose up --build -d
```

This starts a MongoDB 8.0 container with a persistent volume (`mongodb_data`) and waits until it passes a health check before allowing the runner to connect.

Verify it is healthy:

```bash
docker compose ps
```

### 2. Run a simulation

```bash
docker compose run --rm runner python simulations.py \
    --records=200000 \
    --versions=5 \
    --fields=20 \
    --domain=40 \
    --repetitions=3 \
    --evolution_fields=2 \
    --operations=50 \
    --update_percent=0.05 \
    --mode="preprocess" \
    --method="operations_first" \
    --destination="results.csv"
```

The `MONGO_HOST` environment variable is automatically set to `mongodb` (the Docker service name) inside the runner container — no `--host` flag needed.

Results are written to `results.csv` in the project root (mounted as a volume).

### 3. Run the full benchmark battery

```bash
docker compose run --rm runner bash simulations_batch.sh
```

### 4. Run the tests

```bash
docker compose run --rm runner python -m pytest -v
```

Pass extra pytest flags after `-v` as needed — e.g. `-k translation` to run only translation tests, `--tb=short` for compact tracebacks.

### 5. Stop and clean up

```bash
# Stop containers (data volume is preserved)
docker compose down

# Stop and delete all data
docker compose down -v
```

---

## Option C — Docker Replica Set (distributed experiments)

This setup runs a 3-node MongoDB Replica Set inside Docker, which is required for distributed benchmarks, transaction testing, and the distributed test suite.

### 1. Start the replica set

```bash
docker compose -f docker-compose.replicaset.yml up -d \
  mongo-primary mongo-secondary-1 mongo-secondary-2 mongo-init
```

This starts:
- `mongo-primary` on port **27017** (priority 2 — preferred PRIMARY)
- `mongo-secondary-1` on port **27018**
- `mongo-secondary-2` on port **27019**
- `mongo-init` — a one-shot container that runs `rs.initiate(...)` and waits for a PRIMARY to be elected, then exits

Wait ~30 seconds for election, then verify:

```bash
mongosh --port 27017 --eval "rs.status().members.map(m => ({name: m.name, state: m.stateStr}))"
```

Expected output: one `PRIMARY`, two `SECONDARY`.

### 2. Run a simulation against the replica set

```bash
uv run simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=30 --evolution_fields=2 --operations=50 \
    --update_percent=0.05 --mode="preprocess" \
    --method="operations_first" \
    --mongo_uri="mongodb://localhost:27017/?directConnection=true" \
    --write_concern="majority" \
    --nodes=3 \
    --destination="results_rs3_majority.csv"
```

The `--nodes` argument is metadata only — it is written to the output CSV so results from different topologies can be compared. The `--write_concern` argument accepts `1`, `majority`, or `all`.

### 3. Run all tests (unit + distributed)

```bash
# Unit tests (work against any running MongoDB)
uv run pytest semantic_heterogeneous_database/tests/ -v

# Distributed tests — require the replica set to be running on localhost:27017-27019
uv run pytest tests/distributed/ -m "not slow" -v

# Slow distributed tests: failover and consistency rate measurement
uv run pytest tests/distributed/ -m slow -v --timeout=180
```

See the [Running Tests](#running-tests) section for full details.

### 4. Stop the replica set

```bash
# Stop containers (data volumes are preserved)
docker compose -f docker-compose.replicaset.yml down

# Stop and delete all data
docker compose -f docker-compose.replicaset.yml down -v
```

---

## Option B — Local Installation

### 1. Install Python 3.12+

```bash
python3 --version   # must be ≥ 3.12
```

On Ubuntu, if not installed:

```bash
sudo apt update && sudo apt install python3 python3-pip python3-venv build-essential
```

### 2. Install `uv`

```bash
curl -Ls https://astral.sh/uv/install.sh | sh
```

### 3. Install Python dependencies

```bash
uv sync
```

### 4. Install MongoDB 8.0

```bash
# Import MongoDB GPG key
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg --dearmor

# Add the repository
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] \
https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/8.0 multiverse" | \
sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

# Install
sudo apt-get update && sudo apt-get install -y mongodb-org

# Start
sudo systemctl start mongod
```

Verify it is running:

```bash
sudo systemctl status mongod
```

### 5. Run the tests

```bash
uv run python -m pytest -v
```

### 6. Run a simulation

```bash
uv run simulations.py \
    --records=200000 \
    --versions=5 \
    --fields=20 \
    --domain=40 \
    --repetitions=3 \
    --evolution_fields=2 \
    --operations=50 \
    --update_percent=0.05 \
    --mode="preprocess" \
    --method="operations_first" \
    --destination="results.csv"
```

### 7. Stop MongoDB when done

```bash
sudo systemctl stop mongod
```

---

## Running Tests

MellowDB has two test suites. Unit tests require only a running MongoDB instance (single node or replica set). Distributed tests additionally require a 3-node replica set.

### Unit tests

```bash
# Against single-node MongoDB (docker-compose.yml up, or local mongod)
uv run pytest semantic_heterogeneous_database/tests/ -v

# Against the replica set (docker-compose.replicaset.yml up)
uv run pytest semantic_heterogeneous_database/tests/ -v
# No change needed — the conftest detects topology automatically
```

### Distributed tests

Start the replica set first (Option C above), then:

```bash
# Fast distributed tests: semantic correctness and concurrent operation safety
# Auto-skipped if no replica set is reachable
uv run pytest tests/distributed/ -m "not slow" -v
```

```bash
# Slow distributed tests: primary failover (~30-60s each) and consistency rate measurement
uv run pytest tests/distributed/ -m slow -v --timeout=180
```

```bash
# Consistency rate test with CSV output
uv run pytest tests/distributed/test_semantic_consistency_rate.py \
  -m slow -v --output=results_consistency.csv
```

```bash
# Run everything (unit + distributed fast)
uv run pytest semantic_heterogeneous_database/tests/ tests/distributed/ -m "not slow" -v
```

### What the distributed test suite covers

| File | Marks | What it tests |
|------|-------|---------------|
| `test_distributed_correctness.py` | (none) | Semantic correctness of translation, grouping, rewrite mode on a replica set; verifies `_versions_r` routes to PRIMARY with majority concern |
| `test_concurrent_operations.py` | (none) | Concurrent `execute_operation` calls leave the version chain consistent — no duplicate version numbers, no broken pointers |
| `test_failover.py` | `slow` | Primary failure and election: data written before failover survives; new operations work after election |
| `test_semantic_consistency_rate.py` | `slow` | Quantifies semantic correctness (F1 score) under four combinations of write concern and version read concern; asserts majority reads always yield 100% F1 |

---

## Simulation Arguments

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
| `--destination` | string | Output CSV file path |
| `--host` | string | MongoDB host (overrides `MONGO_HOST` env var; default: `localhost`) |
| `--mongo_uri` | string | Full MongoDB URI — use for replica sets, e.g. `mongodb://localhost:27017/?directConnection=true` |
| `--write_concern` | string | Write concern: `1`, `majority` (default), or `all` |
| `--nodes` | int | Number of RS nodes — written to CSV as metadata for experiment comparison |

### Initialization strategies

| Method | Description |
|--------|-------------|
| `operations_first` | Register all semantic evolutions first, then insert records. Faster overall. |
| `insertion_first` | Insert records first, then register evolutions (triggers full reprocessing). Benefits from indexes. |

### Example commands

```bash
# Single-node: read-heavy workload, preprocessing strategy
uv run simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=3 --evolution_fields=2 --operations=500 \
    --update_percent=0 --mode="preprocess" --method="operations_first" \
    --destination="results_preprocess_read_only.csv"

# Single-node: write-heavy workload, query rewriting strategy
uv run simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=3 --evolution_fields=2 --operations=500 \
    --update_percent=0.95 --mode="rewrite" --method="operations_first" \
    --destination="results_rewrite_write_heavy.csv"

# Replica set: 3-node RS, majority write concern, preprocessing strategy
uv run simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=30 --evolution_fields=2 --operations=500 \
    --update_percent=0.05 --mode="preprocess" --method="operations_first" \
    --mongo_uri="mongodb://localhost:27017/?directConnection=true" \
    --write_concern="majority" --nodes=3 \
    --destination="results_rs3_preprocess.csv"
```

For all options:

```bash
uv run simulations.py --help          # local
docker compose run --rm runner python simulations.py --help   # Docker
```

---

## Project Structure

```
├── semantic_heterogeneous_database/   # MellowDB Python package
│   ├── BasicCollection.py             # Public API; accepts mongo_uri + write_concern
│   ├── Collection.py                  # Core engine: insert, find, query rewriting,
│   │                                  #   write concern, _versions_r, _is_replica_set
│   ├── TranslationOperation.py        # 1-to-1 rename; transaction-protected chain writes
│   ├── GroupingOperation.py           # Many-to-1 merge; transaction-protected chain writes
│   ├── UngroupingOperation.py         # 1-to-many split; transaction-protected chain writes
│   ├── SemanticOperation.py           # Abstract base class
│   └── tests/                         # Unit tests (work on any MongoDB topology)
│       ├── conftest.py                # pytest fixtures (make_collection, count)
│       ├── test_translation.py
│       ├── test_grouping.py
│       ├── test_ungrouping.py
│       ├── test_chained.py
│       └── test_edge_cases.py
│
├── tests/
│   └── distributed/                   # Distributed tests (require replica set)
│       ├── conftest.py                # RS fixtures: make_rs_collection, assert_chain_integrity
│       ├── test_distributed_correctness.py   # Semantic correctness on RS
│       ├── test_concurrent_operations.py     # Version chain safety under concurrent ops
│       ├── test_failover.py                  # Primary failover recovery (slow)
│       └── test_semantic_consistency_rate.py # F1 score vs. consistency level (slow)
│
├── docker/
│   ├── runner/
│   │   ├── Dockerfile                 # Python 3.12-slim runner image
│   │   └── requirements.txt
│   └── replicaset/
│       └── init-replicaset.js         # rs.initiate() script for the RS init container
│
├── analysis/
│   ├── datasus/processamento.r        # R script — generates paper figures (real dataset)
│   └── random/processamento.r         # R script — generates paper figures (synthetic data)
│
├── dataset/
│   └── MellowDB - experiments/        # Raw result files from paper experiments
│       ├── first experiment/
│       ├── indexes experiment/
│       └── initialization experiment/
│
├── docs/
│   ├── artigo_fgcs_2025_semantic_evolution_dbs.pdf
│   └── distributed-implementation-notes.md  # Detailed notes on the distributed extension
│
├── database_generator.py              # Synthetic data and operation generator
├── simulations.py                     # Benchmark CLI (now includes --mongo_uri, --write_concern, --nodes)
├── simulations_batch.sh               # Full benchmark battery
├── simulations_realcases.py           # Benchmark using real DATASUS dataset
├── simulations_realcases_operations.py
├── simulations_writer.py              # Result file writer utility
├── tests.py                           # Thin pytest entry-point
├── docker-compose.yml                 # Single-node MongoDB setup
├── docker-compose.replicaset.yml      # 3-node replica set setup
└── pyproject.toml                     # Dependencies and pytest configuration
```

---

## Using MellowDB as a Library

```python
from semantic_heterogeneous_database import BasicCollection
from datetime import datetime

# Single-node (default)
col = BasicCollection('mydb', 'mycollection', operation_mode='preprocess')

# Replica set
col = BasicCollection(
    'mydb', 'mycollection',
    mongo_uri='mongodb://localhost:27017/?directConnection=true',
    operation_mode='preprocess',
    write_concern='majority'
)

# Insert records with their validity date
col.insert_one('{"city": "Piçarras", "population": 50000}', datetime(2000, 1, 1))

# Register a semantic evolution: "Piçarras" was renamed in 2004
col.execute_operation('translation', datetime(2004, 1, 1), {
    'fieldName': 'city',
    'oldValue': 'Piçarras',
    'newValue': 'Balneário Piçarras'
})

# Load many evolution operations from a CSV file
col.execute_many_operations_by_csv('operations.csv', 'type', 'valid_from')

# Query transparently — returns records from all periods under the current name
results = col.find_many({'city': 'Balneário Piçarras'})
col.pretty_print(results)

# Create indexes for better query performance
col.create_index(['city'])
```