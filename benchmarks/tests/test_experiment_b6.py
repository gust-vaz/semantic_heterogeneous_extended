import pandas as pd
from benchmarks.experiments import b6_chain_depth as b6
from benchmarks.harness.results import COLUMNS


def test_chain_lengths_span_one_to_fifty():
    assert b6.CHAIN_LENGTHS == [1, 5, 10, 25, 50]


def test_both_strategies_are_swept():
    assert b6.OPERATION_MODES == ["preprocess", "rewrite"]


def test_cells_are_the_cross_product():
    assert len(b6.cells("rs3")) == 10
    assert (1, "preprocess") in b6.cells("rs3")


def test_b6_smoke_run_reduces_the_chain_sweep(tmp_path, primary_uri, monkeypatch):
    monkeypatch.setenv("BENCH_PRIMARY_URI", primary_uri)
    exit_code = b6.main([
        "--deployment", "single", "--profile", "smoke",
        "--out", str(tmp_path), "--records", "50",
        "--chain-lengths", "1,5",
    ])
    assert exit_code == 0

    frame = pd.read_csv(list(tmp_path.rglob("b6_smoke_*.csv"))[0])
    assert list(frame.columns) == COLUMNS
    assert len(frame) == 4                       # 2 chain lengths x 2 modes x 1 rep
    assert sorted(frame["chain_length"].unique()) == [1, 5]
    assert sorted(frame["operation_mode"].unique()) == ["preprocess", "rewrite"]
    assert (frame["setup_operations_s"] > 0).all()
