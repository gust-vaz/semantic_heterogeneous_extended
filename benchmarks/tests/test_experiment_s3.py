import pytest

from benchmarks.harness.corpus import CorpusHandle
from benchmarks.experiments.s3_targeting import (
    ANCHOR, KEY_COLUMNS, arms, cells, namespace_for, query_sets,
)

EVOLVED = frozenset({"evo0", "evo1"})


def _handle_with(query_set):
    return CorpusHandle(database_name="d", collection_name="c",
                        query_set=query_set, setup_insert_s=1.0,
                        setup_operations_s=1.0, record_count=10)


def test_a_targeted_query_names_the_shard_key():
    sets = query_sets(_handle_with([{"evo0": "a"}, {"evo1": "z"}]), "evo0", EVOLVED)
    assert sets["targeted"] == [{"evo0": "a"}]


def test_a_broadcast_query_does_not_name_the_shard_key():
    sets = query_sets(_handle_with([{"evo0": "a"}, {"evo1": "z"}]), "evo0", EVOLVED)
    assert sets["broadcast"] == [{"evo1": "z"}]


def test_the_broadcast_arm_stays_in_the_shard_keys_own_class():
    """A query naming an evolved field also pays to expand the version chain.
    Mixing classes compares targeting and semantic work at once - measured, it
    made the evolved row's targeted arm come out slower than its broadcast one.
    """
    handle = _handle_with([{"evo0": "a"}, {"evo1": "b"}, {"f1": "z"}])
    assert query_sets(handle, "evo0", EVOLVED)["broadcast"] == [{"evo1": "b"}]
    assert query_sets(handle, "f0", EVOLVED)["broadcast"] == [{"f1": "z"}]


def test_sharding_by_id_leaves_every_semantic_query_a_broadcast():
    """MellowDB's queries name data fields, never _id. A cell sharded on _id
    therefore has no targeted arm at all - which is that row's result, not a
    misconfiguration: it is what choosing a system field as the shard key costs.
    """
    sets = query_sets(_handle_with([{"evo0": "a"}, {"f1": "z"}]), "_id", EVOLVED)
    assert sets["targeted"] == []
    # _id does not evolve, so its broadcast arm is the plain fields
    assert sets["broadcast"] == [{"f1": "z"}]
    assert arms(sets) == ["broadcast"]


def test_a_cell_with_both_kinds_runs_both_arms():
    sets = query_sets(_handle_with([{"evo0": "a"}, {"evo1": "z"}]), "evo0", EVOLVED)
    assert arms(sets) == ["targeted", "broadcast"]


def test_a_corpus_with_no_queries_at_all_is_refused():
    with pytest.raises(RuntimeError) as exc:
        query_sets(_handle_with([]), "evo0", EVOLVED)
    assert "no queries" in str(exc.value)


def test_every_shard_key_runs_under_preprocess_and_the_anchor_adds_rewrite():
    got = cells("sh4")
    assert sum(1 for _, _, mode in got if mode == "preprocess") == 6
    assert sum(1 for _, _, mode in got if mode == "rewrite") == 1
    assert (ANCHOR["role"], ANCHOR["kind"], "rewrite") in got


def test_the_key_columns_separate_the_filter_axis():
    for column in ["experiment", "deployment", "corpus", "operation_mode",
                   "shard_key_role", "shard_key_kind", "mix", "repetition"]:
        assert column in KEY_COLUMNS


def test_the_namespace_follows_the_operation_mode():
    handle = _handle_with([{"evo0": "a"}])
    assert namespace_for(handle, "preprocess") == "d.c_processed"
    assert namespace_for(handle, "rewrite") == "d.c"
