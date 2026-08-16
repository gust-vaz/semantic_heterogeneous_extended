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


def make_row(**values):
    """Build a full-schema row; unspecified columns are empty strings."""
    unknown = set(values) - set(COLUMNS)
    if unknown:
        raise ValueError(
            f"Unknown result column(s): {', '.join(sorted(unknown))}"
        )
    row = {column: "" for column in COLUMNS}
    row.update(values)
    return row


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


def write_rows(path, rows):
    """Append rows to the CSV, writing the header only when creating it."""
    if not rows:
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    is_new = not os.path.exists(path)
    frame = pd.DataFrame(rows, columns=COLUMNS)
    frame.to_csv(path, mode="a", header=is_new, index=False)


def result_path(out_dir, experiment, profile, today=None):
    stamp = today or datetime.now().strftime("%Y%m%d")
    return os.path.join(out_dir, experiment, f"{experiment}_{profile}_{stamp}.csv")
