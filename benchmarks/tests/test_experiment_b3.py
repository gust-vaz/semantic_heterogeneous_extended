import pandas as pd
from benchmarks.experiments import b3_write_replication as b3
from benchmarks.harness.results import COLUMNS


def test_write_concern_sweep_covers_one_majority_and_all():
    assert b3.WRITE_CONCERNS == ["1", "majority", "all"]


def test_single_node_still_sweeps_every_write_concern():
    assert b3.cells("single") == ["1", "majority", "all"]


def test_b3_records_write_latencies_and_no_reads(tmp_path, primary_uri, monkeypatch):
    monkeypatch.setenv("BENCH_PRIMARY_URI", primary_uri)
    exit_code = b3.main([
        "--deployment", "single", "--profile", "smoke",
        "--out", str(tmp_path), "--records", "50",
    ])
    assert exit_code == 0

    frame = pd.read_csv(list(tmp_path.rglob("b3_smoke_*.csv"))[0])
    assert list(frame.columns) == COLUMNS
    assert len(frame) == 3                       # 3 write concerns x 1 rep
    assert sorted(frame["write_concern"].astype(str)) == ["1", "all", "majority"]
    assert frame["write_p95_ms"].notna().all()
    assert frame["read_p95_ms"].isna().all()
    assert frame["mix"].unique().tolist() == ["write_only"]
