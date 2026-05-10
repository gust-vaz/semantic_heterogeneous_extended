# Semantic Heterogeneous Database

This project is a prototype of a semantic evolution compatible database using MongoDB.

## Getting Started (Linux)

Follow these steps to set up and run the project from a fresh Linux installation:

### 1. Install Python

Ensure you have Python 3.12 or newer installed. You can check your version with:

```bash
python3 --version
```

If Python is not installed, install it using your package manager. For example, on Ubuntu:

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv
```

### 2. Install System Dependencies

Some Python packages require system libraries. Install them with:

```bash
sudo apt install build-essential libatlas-base-dev
```

### 3. Install Python Dependencies


All required Python packages are listed in `pyproject.toml` for modern dependency management.

#### Recommended: Using uv

First, install [`uv`](https://github.com/astral-sh/uv) (if not already installed):

```bash
curl -Ls https://astral.sh/uv/install.sh | sh
```

#### Main dependencies:
- pandas: Data analysis and manipulation
- pymongo: MongoDB client
- matplotlib, numpy: Data processing and visualization
- memory-profiler, psutil: Profiling and system monitoring

## MongoDB 8.0 Installation (Ubuntu)

### 1. Import the Public Key

Before downloading, your system needs to trust the MongoDB developers. This command downloads their GPG key:

```bash
curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
    sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
    --dearmor
```

### 2. Create the List File

Create the MongoDB source list file with the correct repository content:

```bash
echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu $(lsb_release -cs)/mongodb-org/8.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
```

### 3. Reload Local Package Database

After adding the source, refresh package metadata:

```bash
sudo apt-get update
```

### 4. Install MongoDB

Install MongoDB and related tools:

```bash
sudo apt-get install -y mongodb-org
```

### 5. Start and Verify

Installing does not automatically start the service. Use `systemctl`:

- Start the service:

    ```bash
    sudo systemctl start mongod
    ```

- Enable starting it on boot:

    ```bash
    sudo systemctl enable mongod
    ```

- Check status:

    ```bash
    sudo systemctl status mongod
    ```

If you see `active (running)`, MongoDB is installed correctly.

### 6. Finish

- Finish the service:
    ```bash
    sudo systemctl stop mongod
    ```

- Disable starting it on boot:
    ```bash
    sudo systemctl disable mongod
    ```


To open the shell:

```bash
mongosh
```

## Run the Project

You can now run the main scripts, for example:

```bash
uv run simulations.py <parameters>
```

or run tests with:

```bash
uv run tests.py <parameters>
```

### Example command:

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
    --method="insertion_first" \
    --destination="results.csv"
```

```bash
uv run simulations.py \
    --records=200000 \
    --versions=5 \
    --fields=20 \
    --domain=40 \
    --repetitions=3 \
    --evolution_fields=2 \
    --operations=500 \
    --update_percent=0 \
    --mode="preprocess" \
    --method="operations_first" \
    --destination="results.csv"
```

#### Arguments:
- `--records`: Number of records to generate (integer)
- `--versions`: Number of versions (integer)
- `--fields`: Number of fields per record (integer)
- `--domain`: Number of possible values per field (integer)
- `--repetitions`: Number of test repetitions (integer)
- `--method`: Either `insertion_first` or `operations_first`
- `--update_percent`: Fraction of operations that are updates (float, e.g., 0.5)
- `--destination`: Output CSV file for results
- `--evolution_fields`: Number of fields that evolve (integer)
- `--operations`: Number of operations to perform (integer)
- `--mode`: Either `preprocess`or `rewrite`

You can adjust these arguments as needed for your experiments. For more details, see the top of `simulations.py` or run:

```bash
uv run simulations.py --help
```
