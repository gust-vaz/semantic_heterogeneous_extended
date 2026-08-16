import pandas as pd
from benchmarks.experiments import b1_read_offloading as b1
from benchmarks.harness.results import COLUMNS


def test_single_deployment_yields_one_cell():
    assert b1.cells("single") == ["primary"]


def test_replica_set_yields_three_cells():
    assert b1.cells("rs3") == ["primary", "secondaries", "secondaries_single_source"]


def test_b1_writes_a_full_schema_csv_on_single_node(tmp_path, primary_uri, monkeypatch):
    monkeypatch.setenv("BENCH_PRIMARY_URI", primary_uri)
    exit_code = b1.main([
        "--deployment", "single", "--profile", "smoke",
        "--out", str(tmp_path), "--records", "50",
    ])
    assert exit_code == 0

    written = list(tmp_path.rglob("b1_smoke_*.csv"))
    assert len(written) == 1
    frame = pd.read_csv(written[0])
    assert list(frame.columns) == COLUMNS
    assert len(frame) == 1                       # 1 cell x 1 rep at smoke
    assert frame["experiment"].unique().tolist() == ["b1"]
    assert frame["read_target"].unique().tolist() == ["primary"]
    assert frame["nodes"].unique().tolist() == [1]
    assert frame["ops"].iloc[0] > 0
