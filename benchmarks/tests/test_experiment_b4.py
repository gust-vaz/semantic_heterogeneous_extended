import pandas as pd
import pytest
from benchmarks.experiments import b4_chain_depth as b4
from benchmarks.harness.results import COLUMNS


def test_chain_lengths_stop_at_ten():
    # Applying a chain to the preprocess corpus costs exponentially more per hop
    # (Collection re-applies every earlier operation, and recurses twice). Measured
    # at 100k records on 2 GB nodes, setup_operations_s went 0.9 s -> 17.9 s ->
    # 1001 s for c = 1, 5, 10 on one node (1.3 -> 28 -> 1264 s on a 3-node set), and
    # c=15 was projected at 10-20 hours per cluster. So the sweep stops at 10.
    assert b4.CHAIN_LENGTHS == [1, 5, 10]


def test_both_strategies_are_swept():
    assert b4.OPERATION_MODES == ["preprocess", "rewrite"]


def test_cells_are_the_cross_product():
    assert len(b4.cells("rs3")) == 6
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


def test_b4_keeps_the_value_domain_sized_for_chain_fifteen(tmp_path, primary_uri, monkeypatch):
    # The first campaign built its corpora with domain_for_chain(15) = 60 values.
    # Lowering the sweep to 10 must not shrink the domain to 40: that would change
    # the records and queries, and the rows already collected would stop being
    # comparable with new ones.
    from benchmarks.harness import corpus as corpus_module
    real_build = corpus_module.build_corpus
    domains = []

    def spy(kind, **kwargs):
        domains.append(kwargs.get("domain"))
        return real_build(kind, **kwargs)

    monkeypatch.setattr(b4.corpus_module, "build_corpus", spy)
    monkeypatch.setenv("BENCH_PRIMARY_URI", primary_uri)
    assert b4.main(["--deployment", "single", "--profile", "smoke",
                    "--out", str(tmp_path), "--records", "50",
                    "--chain-lengths", "2"]) == 0
    assert domains and set(domains) == {60}
    assert b4.VALUE_DOMAIN == 60


def test_b4_refuses_the_real_corpus():
    # The real corpus has its own fixed operations: a chain sweep over it would
    # measure one corpus six times under different labels.
    with pytest.raises(SystemExit) as raised:
        b4.main(["--deployment", "single", "--corpus", "real"])
    assert raised.value.code == 2
