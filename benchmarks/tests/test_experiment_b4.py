import pandas as pd
from benchmarks.experiments import b4_chain_depth as b4
from benchmarks.harness.results import COLUMNS


def test_chain_lengths_stop_at_fifteen():
    # On the chained corpus preprocess cascades: every hop copies the records the
    # earlier hops already copied, and the collection outgrows the cache. At the
    # 100k campaign size on a 2 GB node, chains up to 10 built in minutes, but
    # c=25 was still on hop 14 after 3 hours (the processed collection at 3x the
    # corpus, the node IO-bound), so the sweep stops at 15.
    assert b4.CHAIN_LENGTHS == [1, 5, 10, 15]


def test_both_strategies_are_swept():
    assert b4.OPERATION_MODES == ["preprocess", "rewrite"]


def test_cells_are_the_cross_product():
    assert len(b4.cells("rs3")) == 8
    assert (1, "preprocess") in b4.cells("rs3")


def test_b4_smoke_run_reduces_the_chain_sweep(tmp_path, primary_uri, monkeypatch):
    monkeypatch.setenv("BENCH_PRIMARY_URI", primary_uri)
    exit_code = b4.main([
        "--deployment", "single", "--profile", "smoke",
        "--out", str(tmp_path), "--records", "50",
        "--chain-lengths", "1,5",
    ])
    assert exit_code == 0

    frame = pd.read_csv(list(tmp_path.rglob("b4_smoke_*.csv"))[0])
    assert list(frame.columns) == COLUMNS
    assert len(frame) == 4                       # 2 chain lengths x 2 modes x 1 rep
    assert sorted(frame["chain_length"].unique()) == [1, 5]
    assert sorted(frame["operation_mode"].unique()) == ["preprocess", "rewrite"]
    assert (frame["setup_operations_s"] > 0).all()


def test_b4_builds_chained_corpora_and_labels_its_rows(tmp_path, primary_uri, monkeypatch):
    # Depth only means depth on a chained corpus: on the default one every
    # operation lands on its own values, so no query ever follows more than a hop.
    from benchmarks.harness import corpus as corpus_module
    real_build = corpus_module.build_corpus
    seen = []

    def spy(kind, **kwargs):
        seen.append(kwargs.get("chained"))
        return real_build(kind, **kwargs)

    monkeypatch.setattr(b4.corpus_module, "build_corpus", spy)
    monkeypatch.setenv("BENCH_PRIMARY_URI", primary_uri)
    assert b4.main(["--deployment", "single", "--profile", "smoke",
                    "--out", str(tmp_path), "--records", "50",
                    "--chain-lengths", "2"]) == 0

    assert seen and all(flag is True for flag in seen)
    frame = pd.read_csv(list(tmp_path.rglob("b4_smoke_*.csv"))[0])
    # Rows say which corpus they measured, so they cannot be mistaken for (or
    # resumed against) the earlier independent-operations B4 rows.
    assert set(frame["corpus"]) == {"synthetic_chained"}
