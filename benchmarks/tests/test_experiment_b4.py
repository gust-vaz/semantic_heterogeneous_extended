import pandas as pd
from benchmarks.experiments import b4_strategy_distribution as b4
from benchmarks.harness.results import COLUMNS


def test_mixes_are_read_ratios():
    assert b4.MIXES == {"read_heavy": 0.95, "write_heavy": 0.05}


def test_single_node_contributes_four_cells():
    # 2 modes x 1 read target x 2 mixes
    assert len(b4.cells("single")) == 4


def test_replica_set_contributes_eight_cells():
    # 2 modes x 2 read targets x 2 mixes
    assert len(b4.cells("rs3")) == 8
    assert ("preprocess", "secondaries", "read_heavy") in b4.cells("rs3")


def test_single_source_target_is_not_swept():
    assert all(target != "secondaries_single_source"
               for _, target, _ in b4.cells("rs3"))


def test_b4_reports_both_read_and_write_latencies(tmp_path, primary_uri, monkeypatch):
    monkeypatch.setenv("BENCH_PRIMARY_URI", primary_uri)
    exit_code = b4.main([
        "--deployment", "single", "--profile", "smoke",
        "--out", str(tmp_path), "--records", "50",
        "--mix", "read_heavy",
    ])
    assert exit_code == 0

    frame = pd.read_csv(list(tmp_path.rglob("b4_smoke_*.csv"))[0])
    assert list(frame.columns) == COLUMNS
    assert len(frame) == 2                       # 2 modes x 1 target x 1 mix x 1 rep
    assert frame["mix"].unique().tolist() == ["read_heavy"]
    assert (frame["setup_insert_s"] > 0).all()
    assert sorted(frame["operation_mode"].unique()) == ["preprocess", "rewrite"]
