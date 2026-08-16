import json
from datetime import datetime

import pytest
from pymongo import MongoClient
from benchmarks.harness.corpus import build_synthetic, domain_for_chain


def test_domain_grows_with_chain_length():
    # DatabaseGenerator never reuses an evolved value, so a long chain needs a
    # correspondingly large domain or generate_version() recurses forever.
    assert domain_for_chain(1) == 20        # floor
    assert domain_for_chain(50) == 200
    assert domain_for_chain(25) < domain_for_chain(50)


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


def test_same_seed_produces_the_same_query_set(primary_uri, cleanup_corpus):
    first = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=10, chain_length=0,
        operation_mode="preprocess", seed=7))
    second = cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=10, chain_length=0,
        operation_mode="preprocess", seed=7))

    assert first.query_set == second.query_set
    assert first.database_name != second.database_name
