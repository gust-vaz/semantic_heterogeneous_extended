import os

import pandas as pd

from benchmarks.harness.resume import cell_key, completed_cells


def _write(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


KEYS = ["deployment", "read_target", "repetition"]


def test_cell_key_normalises_to_strings():
    # a CSV round-trip turns 0 into "0"; the key must match either way
    assert cell_key({"deployment": "rs3", "read_target": "primary", "repetition": 0},
                    KEYS) == ("rs3", "primary", "0")


def test_cell_key_treats_a_missing_column_as_empty():
    assert cell_key({"deployment": "rs3"}, KEYS) == ("rs3", "", "")


def test_repetitions_are_distinct_cells():
    a = cell_key({"deployment": "rs3", "read_target": "primary", "repetition": 0}, KEYS)
    b = cell_key({"deployment": "rs3", "read_target": "primary", "repetition": 1}, KEYS)
    assert a != b


def test_completed_cells_is_empty_when_nothing_ran(tmp_path):
    assert completed_cells(str(tmp_path), "b1", "small", KEYS) == set()


def test_completed_cells_reads_rows_already_written(tmp_path):
    _write(str(tmp_path / "b1" / "b1_small_20260920.csv"),
           [{"profile": "small", "deployment": "rs3",
             "read_target": "primary", "repetition": 0}])
    assert completed_cells(str(tmp_path), "b1", "small", KEYS) == {("rs3", "primary", "0")}


def test_completed_cells_ignores_another_profile(tmp_path):
    _write(str(tmp_path / "b1" / "b1_smoke_20260920.csv"),
           [{"profile": "smoke", "deployment": "rs3",
             "read_target": "primary", "repetition": 0}])
    assert completed_cells(str(tmp_path), "b1", "small", KEYS) == set()


def test_completed_cells_spans_files_from_different_days(tmp_path):
    """A campaign is run one session per day, and result_path stamps the filename
    with the date. Scanning only today's file would re-run everything."""
    _write(str(tmp_path / "b1" / "b1_small_20260920.csv"),
           [{"profile": "small", "deployment": "rs3",
             "read_target": "primary", "repetition": 0}])
    _write(str(tmp_path / "b1" / "b1_small_20260921.csv"),
           [{"profile": "small", "deployment": "rs5",
             "read_target": "primary", "repetition": 0}])
    assert completed_cells(str(tmp_path), "b1", "small", KEYS) == {
        ("rs3", "primary", "0"), ("rs5", "primary", "0")}
