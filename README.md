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
docker compose run --rm runner python tests.py
```

### 5. Stop and clean up

```bash
# Stop containers (data volume is preserved)
docker compose down

# Stop and delete all data
docker compose down -v
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

### 5. Run a simulation

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

### 6. Stop MongoDB when done

```bash
sudo systemctl stop mongod
```

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

### Initialization strategies

| Method | Description |
|--------|-------------|
| `operations_first` | Register all semantic evolutions first, then insert records. Faster overall. |
| `insertion_first` | Insert records first, then register evolutions (triggers full reprocessing). Benefits from indexes. |

### Example commands

```bash
# Read-heavy workload, preprocessing strategy, operations-first init
uv run simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=3 --evolution_fields=2 --operations=500 \
    --update_percent=0 --mode="preprocess" --method="operations_first" \
    --destination="results_preprocess_read_only.csv"

# Write-heavy workload, query rewriting strategy
uv run simulations.py \
    --records=200000 --versions=5 --fields=20 --domain=40 \
    --repetitions=3 --evolution_fields=2 --operations=500 \
    --update_percent=0.95 --mode="rewrite" --method="operations_first" \
    --destination="results_rewrite_write_heavy.csv"
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
│   ├── BasicCollection.py             # Public API (PyMongo-compatible)
│   ├── Collection.py                  # Core engine: insert, find, query rewriting
│   ├── TranslationOperation.py        # 1-to-1 rename (reversible)
│   ├── GroupingOperation.py           # Many-to-1 merge (forward only)
│   ├── UngroupingOperation.py         # 1-to-many split (backward only)
│   ├── SemanticOperation.py           # Abstract base class
│   └── tests/                         # Unit tests
│
├── docker/
│   └── runner/
│       ├── Dockerfile                 # Python 3.12-slim runner image
│       └── requirements.txt           # Pinned Python dependencies
│
├── analysis/
│   ├── datasus/processamento.r        # R script — generates paper figures (real dataset)
│   └── random/processamento.r         # R script — generates paper figures (synthetic data)
│
├── dataset/
│   └── MellowDB - experiments/        # Raw result files from paper experiments
│       ├── first experiment/          # Preprocess vs. rewrite, no indexes
│       ├── indexes experiment/        # With and without MongoDB indexes
│       └── initialization experiment/ # operations_first vs. insertion_first
│
├── docs/
│   └── artigo_fgcs_2025_semantic_evolution_dbs.pdf
│
├── database_generator.py              # Synthetic data and operation generator
├── simulations.py                     # Main benchmark CLI
├── simulations_batch.sh               # Full benchmark battery (paper experiments)
├── simulations_realcases.py           # Benchmark using real DATASUS dataset
├── simulations_realcases_operations.py
├── simulations_writer.py              # Result file writer utility
├── tests.py                           # Test runner
├── docker-compose.yml
└── pyproject.toml                     # Dependencies (uv / pip)
```

---

## Using MellowDB as a Library

```python
from semantic_heterogeneous_database import BasicCollection
from datetime import datetime

# Choose 'preprocess' (eager) or 'rewrite' (lazy)
col = BasicCollection('mydb', 'mycollection', host='localhost', operation_mode='preprocess')

# Insert records with their validity date
col.insert_one('{"city": "Piçarras", "population": 50000}', datetime(2000, 1, 1))

# Register a semantic evolution: "Piçarras" was renamed in 2004
col.collection.execute_operation('translation', datetime(2004, 1, 1), {
    'fieldName': 'city',
    'oldValue': 'Piçarras',
    'newValue': 'Balneário Piçarras'
})

# Load many evolution operations from a CSV file
col.collection.execute_many_operations_by_csv('operations.csv', 'type', 'valid_from')

# Query transparently — returns records from all periods under the current name
results = col.collection.find_many({'city': 'Balneário Piçarras'})
col.collection.pretty_print(results)

# Create indexes for better query performance
col.collection.create_index(['city'])
```