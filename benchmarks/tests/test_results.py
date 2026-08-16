import os
import pandas as pd
import pytest
from benchmarks.harness.results import (
    COLUMNS, make_row, run_metadata, write_rows, result_path,
)


def test_schema_contains_the_identifying_and_metric_columns():
    for column in ["experiment", "profile", "deployment", "nodes", "corpus",
                   "repetition", "operation_mode", "write_concern", "read_mode",
                   "read_target", "clients", "records", "chain_length", "mix",
                   "warmup_s", "measure_s", "ops", "errors", "error_rate",
                   "throughput_ops_s", "mean_ms", "p50_ms", "p95_ms", "p99_ms",
                   "read_p95_ms", "write_p95_ms", "setup_insert_s",
                   "setup_operations_s", "started_at", "git_sha",
                   "mongo_version", "host_cpus", "host_mem_gb"]:
        assert column in COLUMNS


def test_make_row_fills_missing_columns_with_empty_string():
    row = make_row(experiment="b1", ops=10)
    assert set(row) == set(COLUMNS)
    assert row["experiment"] == "b1"
    assert row["ops"] == 10
    assert row["chain_length"] == ""


def test_make_row_rejects_unknown_columns():
    with pytest.raises(ValueError) as exc:
        make_row(experiment="b1", nonsense=1)
    assert "nonsense" in str(exc.value)


def test_run_metadata_reports_the_machine():
    meta = run_metadata(mongo_version="8.0.12")
    assert meta["mongo_version"] == "8.0.12"
    assert meta["host_cpus"] >= 1
    assert meta["host_mem_gb"] > 0
    assert meta["started_at"]
    assert "git_sha" in meta


def test_run_metadata_prefers_the_git_sha_from_the_environment(monkeypatch):
    monkeypatch.setenv("BENCH_GIT_SHA", "abc1234")
    assert run_metadata()["git_sha"] == "abc1234"


def test_write_rows_creates_the_file_with_a_header(tmp_path):
    path = str(tmp_path / "nested" / "out.csv")
    write_rows(path, [make_row(experiment="b1", ops=5)])
    frame = pd.read_csv(path)
    assert list(frame.columns) == COLUMNS
    assert len(frame) == 1


def test_write_rows_appends_without_repeating_the_header(tmp_path):
    path = str(tmp_path / "out.csv")
    write_rows(path, [make_row(experiment="b1", ops=5)])
    write_rows(path, [make_row(experiment="b1", ops=6)])
    frame = pd.read_csv(path)
    assert len(frame) == 2
    assert list(frame["ops"]) == [5, 6]


def test_write_rows_ignores_an_empty_batch(tmp_path):
    path = str(tmp_path / "out.csv")
    write_rows(path, [])
    assert not os.path.exists(path)


def test_result_path_encodes_experiment_profile_and_date():
    path = result_path("results", "b1", "small", today="20260816")
    assert path == os.path.join("results", "b1", "b1_small_20260816.csv")
