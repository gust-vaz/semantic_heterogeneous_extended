import json
import threading
from datetime import datetime

import pytest
from pymongo import MongoClient
from benchmarks.harness.corpus import build_synthetic, domain_for_chain
from semantic_heterogeneous_database import sharding


def test_domain_grows_with_chain_length():
    # DatabaseGenerator never reuses an evolved value, so a long chain needs a
    # correspondingly large domain or generate_version() recurses forever.
    assert domain_for_chain(1) == 20        # floor
    assert domain_for_chain(50) == 200
    assert domain_for_chain(25) < domain_for_chain(50)


def test_build_synthetic_hands_the_shard_key_to_the_library(primary_uri, cleanup_corpus):
    # Off a cluster the library warns that it ignored the key; that warning proves
    # the key travelled from the corpus builder all the way into MellowDB.
    if sharding.is_mongos(MongoClient(primary_uri)):
        pytest.skip("asserts the off-cluster warning")
    with pytest.warns(RuntimeWarning, match="evo0"):
        handle = build_synthetic(primary_uri, records=5, chain_length=1,
                                 operation_mode="preprocess", shard_key={"evo0": "hashed"})
    cleanup_corpus(primary_uri, handle)


def test_too_small_a_domain_is_rejected_up_front(primary_uri):
    with pytest.raises(ValueError) as exc:
        build_synthetic(primary_uri, records=10, chain_length=50,
                        operation_mode="preprocess", domain=20)
    assert "domain" in str(exc.value)


def test_a_long_chain_builds_without_exhausting_the_domain(primary_uri, cleanup_corpus):
    # Regression: chain_length=50 with the old fixed domain of 20 raised
    # RecursionError inside DatabaseGenerator.generate_version().
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=20, chain_length=50, operation_mode="preprocess"))
    assert handle.record_count == 20


def test_synthetic_corpus_inserts_the_requested_records(primary_uri, cleanup_corpus):
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=50, chain_length=0, operation_mode="preprocess"))

    client = MongoClient(primary_uri)
    stored = client[handle.database_name][handle.collection_name].count_documents({})
    assert stored == 50
    assert handle.record_count == 50


def test_synthetic_corpus_reports_both_setup_phases(primary_uri, cleanup_corpus):
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=20, chain_length=2, operation_mode="preprocess"))

    assert handle.setup_insert_s > 0
    assert handle.setup_operations_s > 0


def test_synthetic_query_set_matches_real_records(primary_uri, cleanup_corpus):
    from semantic_heterogeneous_database import BasicCollection

    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=100, chain_length=0, operation_mode="preprocess"))

    assert len(handle.query_set) > 0
    collection = BasicCollection(handle.database_name, handle.collection_name,
                                 primary_uri, "preprocess")
    matched = sum(collection.count_documents(q) for q in handle.query_set)
    assert matched > 0, "query set must actually hit records"


def test_make_record_produces_insertable_pairs(primary_uri, cleanup_corpus):
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=10, chain_length=0, operation_mode="preprocess"))

    payload, valid_from = handle.make_record()
    assert isinstance(payload, str)
    assert isinstance(json.loads(payload), dict)
    assert isinstance(valid_from, datetime)


def _raw_records(primary_uri, handle):
    raw = MongoClient(primary_uri)[handle.database_name][handle.collection_name]
    return sorted(str(doc) for doc in raw.find({}, {"_id": 0}))


def _registered_operations(primary_uri, handle):
    # Version numbers are left out on purpose: SemanticOperation draws them from the
    # global random module by design, and they were never part of a seeded corpus.
    versions = MongoClient(primary_uri)[handle.database_name][handle.collection_name + "_versions"]
    return sorted(str((doc["version_valid_from"], doc["previous_operation"]))
                  for doc in versions.find({"previous_operation": {"$ne": None}}))


def test_same_seed_produces_the_same_corpus_despite_concurrent_traffic(primary_uri, cleanup_corpus):
    """pymongo draws request ids from the global random module - from the calling
    thread and from its monitor threads alike - so any MongoDB traffic in the process
    advances a globally seeded sequence. A seeded corpus must come out the same anyway,
    or no run is reproducible."""
    stop = threading.Event()

    def chatter():
        client = MongoClient(primary_uri)
        while not stop.is_set():
            client.admin.command("ping")

    thread = threading.Thread(target=chatter, daemon=True)
    thread.start()
    try:
        first = cleanup_corpus(primary_uri, build_synthetic(
            primary_uri, records=10, chain_length=2,
            operation_mode="preprocess", seed=7))
        second = cleanup_corpus(primary_uri, build_synthetic(
            primary_uri, records=10, chain_length=2,
            operation_mode="preprocess", seed=7))
    finally:
        stop.set()
        thread.join(timeout=5)

    assert first.query_set == second.query_set
    assert _raw_records(primary_uri, first) == _raw_records(primary_uri, second)
    assert _registered_operations(primary_uri, first) == _registered_operations(primary_uri, second)
    assert first.database_name != second.database_name

def _value_counts(uri, handle, field):
    from collections import Counter
    raw = MongoClient(uri)[handle.database_name][handle.collection_name]
    return Counter(document[field] for document in raw.find({}, {field: 1}))


def test_without_skew_values_are_drawn_uniformly(primary_uri, cleanup_corpus):
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=2000, chain_length=0, operation_mode="preprocess",
        domain=20, skew=0.0))
    counts = _value_counts(primary_uri, handle, "evo0")
    assert len(counts) == 20
    assert max(counts.values()) < 3 * min(counts.values())


def test_skew_concentrates_the_domain(primary_uri, cleanup_corpus):
    """A uniform corpus gives every shard key a perfect spread, so no hotspot
    exists to observe and half of what a shard key choice costs disappears."""
    handle = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=2000, chain_length=0, operation_mode="preprocess",
        domain=20, skew=1.5))
    counts = _value_counts(primary_uri, handle, "evo0")
    assert max(counts.values()) > 10 * min(counts.values())


def test_the_same_seed_and_skew_produce_the_same_corpus(primary_uri, cleanup_corpus):
    first = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=200, chain_length=0, operation_mode="preprocess",
        domain=20, skew=1.5, seed=11))
    second = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=200, chain_length=0, operation_mode="preprocess",
        domain=20, skew=1.5, seed=11))
    assert (_value_counts(primary_uri, first, "evo0")
            == _value_counts(primary_uri, second, "evo0"))


def test_skew_does_not_bias_which_values_operations_evolve(primary_uri):
    """Records are drawn skewed; semantic operations keep drawing uniformly.
    Coverage is a separate axis, chosen from the corpus once it exists.

    Asserted on the MEAN rank of the values operations touch, not the maximum.
    Measured over eight seeds at skew 2.0 with a 60-value domain: uniform
    operations gave mean ranks 22.6-35.3, weighted ones 5.5-12.1 - no overlap.
    The maxima overlap almost completely (55-59 against 17-58), because
    __check_evolution refuses to reuse a value and its retry loop pushes even a
    weighted draw out to the tail. An assertion on the maximum passes either way.

    The domain is sized for the chain: generate_version refuses to evolve a
    value twice, and too small a domain recurses until RecursionError.
    """
    import random
    import statistics

    from benchmarks.database_generator import DatabaseGenerator

    domain_size = 60
    means = []
    for seed in range(4):
        generator = DatabaseGenerator(host=primary_uri, rng=random.Random(seed),
                                      skew=2.0)
        generator.generate(number_of_records=0, number_of_versions=1,
                           number_of_fields=4,
                           number_of_values_in_domain=domain_size,
                           number_of_evolution_fields=2,
                           operation_mode="preprocess")
        try:
            assert generator.field_weights, "records should draw from a weighted domain"
            for _ in range(10):
                generator.generate_version()

            ranks = []
            for _, _, arguments in generator.operations:
                if not arguments:
                    continue
                order = generator.field_domain[arguments["fieldName"]]
                for key in ("oldValue", "newValue"):
                    if key in arguments:
                        ranks.append(order.index(arguments[key]))
                for key in ("oldValues", "newValues"):
                    ranks += [order.index(value) for value in arguments.get(key, [])]
            assert ranks, "no operation was generated"
            means.append(statistics.mean(ranks))
        finally:
            generator.destroy()

    # midway between the two measured bands
    assert statistics.mean(means) >= 18, (
        f"operations look drawn from the weighted domain: mean ranks {means}")
