"""The chained synthetic corpus that B4 measures version-chain depth on.

The default corpus applies `chain_length` independent operations: the generator
never reuses a value, so a queried value goes through at most one operation and
"depth" is really operation count. The chained corpus builds one linear
translation chain v0 -> v1 -> ... -> vN on a single field and points every query
at a chain value, so query rewriting has to follow all N hops.
"""
import json
import os

from pymongo import MongoClient

from benchmarks.harness.corpus import build_synthetic, domain_for_chain
from semantic_heterogeneous_database import BasicCollection

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures",
                       "synthetic_default_corpus.json")


def _chain_operations(uri, handle):
    versions = MongoClient(uri)[handle.database_name][handle.collection_name + "_versions"]
    return [v["next_operation"] | {"valid_from": v["next_version_valid_from"]}
            for v in versions.find({"next_operation": {"$ne": None}}).sort("version_valid_from", 1)]


def _snapshot(uri, handle):
    """Same shape as the fixture: what a seeded default corpus is made of."""
    db = MongoClient(uri)[handle.database_name]
    ops = [[op["type"], op["field"], str(op["from"]), str(op["to"]), str(op["valid_from"])]
           for op in _chain_operations(uri, handle)]
    records = [str(r) for r in db[handle.collection_name].find({}, {"_id": 0})
               .sort([("valid_from_date", 1)]).limit(5)]
    return {"query_set": [[k, str(v)] for q in handle.query_set for k, v in q.items()],
            "operations": ops, "first_records": records}


def test_the_default_corpus_is_unchanged(primary_uri, cleanup_corpus):
    # B1, B3, S3 and S4 build their corpora through the default path, and their
    # results were already collected. The chained option must not move a single
    # draw of the seeded generator there: same operations, same queries, same
    # records as the snapshot taken before the option existed.
    expected = json.load(open(FIXTURE))
    for chain_length in (0, 5):
        handle = cleanup_corpus(primary_uri, build_synthetic(
            primary_uri, records=30, chain_length=chain_length, operation_mode="rewrite"))
        assert _snapshot(primary_uri, handle) == expected[str(chain_length)], chain_length


def test_a_chained_corpus_is_one_linear_translation_chain(primary_uri, cleanup_corpus):
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=30, chain_length=5, operation_mode="rewrite",
        domain=domain_for_chain(50), chained=True))
    ops = _chain_operations(primary_uri, handle)

    assert len(ops) == 5
    assert {op["type"] for op in ops} == {"translation"}
    assert len({op["field"] for op in ops}) == 1
    # each hop starts where the previous one ended: v0 -> v1 -> ... -> v5
    for previous, current in zip(ops, ops[1:]):
        assert current["from"] == previous["to"]
    dates = [op["valid_from"] for op in ops]
    assert dates == sorted(dates) and len(set(dates)) == len(dates)
    values = [ops[0]["from"]] + [op["to"] for op in ops]
    assert len(set(values)) == 6                 # a chain, never a cycle


def test_every_chained_query_targets_a_chain_value(primary_uri, cleanup_corpus):
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=30, chain_length=5, operation_mode="rewrite",
        domain=domain_for_chain(50), chained=True))
    ops = _chain_operations(primary_uri, handle)
    field = ops[0]["field"]
    chain = {ops[0]["from"]} | {op["to"] for op in ops}

    assert handle.query_set
    for query in handle.query_set:
        assert list(query) == [field]
        assert query[field] in chain


def test_both_strategies_get_the_same_corpus_and_queries(primary_uri, cleanup_corpus):
    # B4 compares the strategies cell by cell, so the seed must build the same
    # chain and the same queries whichever strategy materialises it.
    handles = {mode: cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=50, chain_length=5, operation_mode=mode,
        domain=domain_for_chain(50), chained=True))
        for mode in ("preprocess", "rewrite")}
    assert handles["preprocess"].query_set == handles["rewrite"].query_set
    assert ([op | {"valid_from": None} for op in _chain_operations(primary_uri, handles["preprocess"])]
            == [op | {"valid_from": None} for op in _chain_operations(primary_uri, handles["rewrite"])])


def test_rewrite_resolves_every_value_of_the_chain(primary_uri, cleanup_corpus):
    # What B4 measures is the cost of following the chain at query time, so the
    # rewrite strategy must actually reach records through it for every value.
    #
    # The two strategies are NOT asserted equal: with two or more chained
    # translations preprocess no longer finds the original value - a limitation
    # the library documents and pins itself in
    # semantic_heterogeneous_database/tests/test_chained.py
    # (test_a_to_b_to_c_preprocess).
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=200, chain_length=3, operation_mode="rewrite",
        domain=domain_for_chain(50), chained=True))
    ops = _chain_operations(primary_uri, handle)
    field = ops[0]["field"]
    collection = BasicCollection(handle.database_name, handle.collection_name,
                                 primary_uri, "rewrite")
    for value in [ops[0]["from"]] + [op["to"] for op in ops]:
        assert len(list(collection.find_many({field: value}))) > 0, value
