"""Tidy, pandas-ready result rows.

One shared wide schema across every experiment so all CSVs concatenate and
filter cleanly. Columns that do not apply to an experiment are written empty.
"""

import os
import subprocess
from datetime import datetime

import pandas as pd
import psutil

COLUMNS = [
    "experiment", "profile", "deployment", "nodes", "corpus", "repetition",
    "operation_mode", "write_concern", "read_mode", "read_target", "clients",
    "records", "chain_length", "mix", "warmup_s", "measure_s",
    "ops", "errors", "error_rate", "throughput_ops_s",
    "mean_ms", "p50_ms", "p95_ms", "p99_ms",
    "read_p95_ms", "write_p95_ms", "setup_insert_s", "setup_operations_s",
    "started_at", "git_sha", "mongo_version", "host_cpus", "host_mem_gb",
]

#: The S series gets a schema of its own rather than columns bolted onto COLUMNS.
#: The two sets of axes are disjoint - read_target, read_mode and write_concern
#: mean nothing on single-node shards, and shard key shape, chunk size and
#: balancer state mean nothing on a replica set - so one wide schema would leave
#: every row of both series half empty.
SHARD_COLUMNS = [
    # identity
    "experiment", "profile", "corpus", "repetition", "deployment", "shards",
    "operation_mode",
    # sharding axes
    "shard_key_field", "shard_key_kind", "shard_key_role",
    "shard_key_cardinality", "skew", "missing_fraction",
    "chunk_size_mb", "presplit_chunks", "balancer",
    # one-shot event measurements (S1, S2, S5)
    "apply_s", "docs_before", "docs_after", "docs_written", "docs_relocated",
    "dist_before", "dist_after", "imbalance_before", "imbalance_after",
    "chunks_before", "chunks_after", "chunks_moved", "jumbo_chunks",
    "bytes_moved", "orphans_after", "converge_s",
    # steady-state measurements (S3, S4)
    "clients", "records", "chain_length", "mix", "warmup_s", "measure_s",
    "ops", "errors", "error_rate", "throughput_ops_s",
    "mean_ms", "p50_ms", "p95_ms", "p99_ms",
    "read_p95_ms", "write_p95_ms", "shards_touched",
    "setup_insert_s", "setup_operations_s",
    # provenance
    "started_at", "git_sha", "mongo_version", "host_cpus", "host_mem_gb",
]


def _row(columns, values):
    unknown = set(values) - set(columns)
    if unknown:
        raise ValueError(
            f"Unknown result column(s): {', '.join(sorted(unknown))}"
        )
    row = {column: "" for column in columns}
    row.update(values)
    return row


def make_row(**values):
    """Build a full B-series row; unspecified columns are empty strings."""
    return _row(COLUMNS, values)


def make_shard_row(**values):
    """Build a full S-series row; unspecified columns are empty strings."""
    return _row(SHARD_COLUMNS, values)


def _git_sha():
    """Short commit sha. bench.sh passes it in because the runner image has no git."""
    from_env = os.environ.get("BENCH_GIT_SHA")
    if from_env:
        return from_env
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL, text=True,
        ).strip()
    except Exception:
        return ""


def run_metadata(mongo_version=""):
    """Provenance columns: which code and which machine produced this row."""
    return {
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "git_sha": _git_sha(),
        "mongo_version": mongo_version,
        "host_cpus": os.cpu_count() or 1,
        "host_mem_gb": round(psutil.virtual_memory().total / 1024 ** 3, 2),
    }


def write_rows(path, rows, columns=COLUMNS):
    """Append rows to the CSV, writing the header only when creating it."""
    if not rows:
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    is_new = not os.path.exists(path)
    frame = pd.DataFrame(rows, columns=columns)
    frame.to_csv(path, mode="a", header=is_new, index=False)


def result_path(out_dir, experiment, profile, today=None):
    stamp = today or datetime.now().strftime("%Y%m%d")
    return os.path.join(out_dir, experiment, f"{experiment}_{profile}_{stamp}.csv")
