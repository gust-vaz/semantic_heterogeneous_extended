import pytest

from benchmarks.experiments.s1_operation_cost import (
    ANCHOR, FIELDS, KEY_COLUMNS, SKEW_LEVELS, cells, pick_most_frequent_value,
)


class _FakeCollection:
    """Enough of a collection for pick_most_frequent_value: a $group count."""

    def __init__(self, counts):
        self._counts = counts

    def aggregate(self, pipeline):
        return [{"_id": value, "n": n} for value, n in self._counts.items()]


def test_the_operation_evolves_whichever_value_the_corpus_has_most_of():
    assert pick_most_frequent_value(_FakeCollection({"a": 80, "b": 15, "c": 5}),
                                    "cid") == "a"


def test_a_uniform_corpus_still_yields_a_value():
    assert pick_most_frequent_value(_FakeCollection({"a": 10, "b": 10}),
                                    "cid") in {"a", "b"}


def test_coverage_is_not_an_axis():
    """Measured: asking a uniform corpus of domain 20 for 80% coverage touched
    6.3% of it, because every value covers about 1/cardinality. Skew is what
    moves coverage - 6.1% at 0.0, 46.0% at 1.5, 83.2% at 3.0 - so skew is the
    axis and how much an operation touched is read back from the row as
    docs_written / docs_before."""
    assert "coverage" not in KEY_COLUMNS
    assert "coverage" not in ANCHOR
    assert SKEW_LEVELS == [0.0, 1.5, 3.0]


def test_the_anchor_sits_at_the_datasus_like_coverage():
    # skew 3.0 puts the most frequent value at about 83%, the range cid sits in
    assert ANCHOR["skew"] == 3.0


def test_the_key_columns_separate_every_axis_the_sweep_varies():
    for column in ["experiment", "deployment", "corpus", "operation_mode",
                   "shard_key_role", "shard_key_kind", "skew",
                   "shard_key_cardinality", "repetition"]:
        assert column in KEY_COLUMNS


def test_the_anchor_is_the_six_keys_across_the_deployment():
    anchor = cells("sh4", anchor_only=True)
    assert len(anchor) == 6
    assert {(cell["role"], cell["kind"]) for cell in anchor} == {
        (role, kind) for role in ("id", "data", "evolved")
        for kind in ("hashed", "ranged")}


def test_every_anchor_cell_sits_at_the_anchor_point():
    for cell in cells("sh4", anchor_only=True):
        assert cell["skew"] == ANCHOR["skew"]
        assert cell["cardinality"] == ANCHOR["cardinality"]


def test_a_deployment_that_is_not_the_anchor_runs_only_the_core():
    # the one-factor-at-a-time extensions are anchored on sh4, so sh1 and sh8
    # contribute the shard-key x scale core and nothing else
    assert len(cells("sh1")) == 6
    assert len(cells("sh8")) == 6


def test_the_anchor_deployment_extends_one_factor_at_a_time():
    extended = cells("sh4")
    assert len(extended) > 6
    for cell in extended[6:]:
        differing = sum(cell[key] != ANCHOR[key] for key in ("skew", "cardinality"))
        assert differing == 1, cell


def test_skew_is_swept_across_every_shard_key():
    """Skew changes how much of the collection the operation touches, which
    every shard key feels - not only the ranged ones."""
    skewed = [c for c in cells("sh4") if c["skew"] != ANCHOR["skew"]]
    assert {(c["role"], c["kind"]) for c in skewed} == {
        (role, kind) for role in ("id", "data", "evolved")
        for kind in ("hashed", "ranged")}


def test_the_roles_name_a_system_field_an_untouched_field_and_the_evolving_one():
    assert FIELDS["id"] == "_id"
    assert FIELDS["evolved"] != FIELDS["data"] != FIELDS["id"]


def test_each_repetition_draws_a_different_corpus(monkeypatch):
    """A translation sends every copy to the one shard its new value routes to,
    so whether anything relocated is a single coin flip on one string's hash.
    With a fixed seed that flip comes out the same every time, and three
    repetitions report one observation three times."""
    from benchmarks.experiments import s1_operation_cost as s1

    seeds = []

    def _capture(kind, **kwargs):
        seeds.append(kwargs["seed"])
        raise RuntimeError("stop after the corpus request")

    monkeypatch.setattr(s1.corpus_module, "build_corpus", _capture)
    monkeypatch.setattr(s1.cell_setup, "prepare", lambda *a, **k: None)
    monkeypatch.setattr(s1.topology, "write_uri", lambda name: "mongodb://x:27017")

    cell = cells("sh4", anchor_only=True)[0]
    args = type("Args", (), {"corpus": "synthetic", "deployment": "sh4",
                             "chunk_size_mb": 1, "profile": "smoke"})()
    for repetition in range(3):
        with pytest.raises(RuntimeError):
            s1.measure_cell(None, args, {"records": 10, "real_max_files": 1},
                            cell, repetition, "8.0.12")
    assert len(set(seeds)) == 3
