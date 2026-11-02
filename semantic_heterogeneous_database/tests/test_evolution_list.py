"""
A preprocess query for a term that has since evolved must keep finding its records
after the current version moves on.

A processed copy of an evolved record only matches its old term through
_evolution_list, which holds the _id of the version owning the operation.
Regression context: d5d23b1 stopped writing those ids, so the old term only looked
right while its operation was the newest one, through the inclusive version bound.
"""
from datetime import datetime

import pandas as pd
import pytest

INSERT_PATHS = ["insert_one", "bulk"]


def _insert(col, how, city, country="X", valid_from=datetime(1900, 1, 1)):
    if how == "insert_one":
        col.insert_one(f'{{"city": "{city}", "country": "{country}"}}', valid_from)
    else:
        col.insert_many_by_dataframe(pd.DataFrame(
            [{"city": city, "country": country, "valid_from_date": valid_from}]),
            "valid_from_date")


def _translate(col, field, old, new, year):
    col.execute_operation("translation", datetime(year, 1, 1),
                          {"fieldName": field, "oldValue": old, "newValue": new})


def _found(col, query):
    """The original records a query finds, ignoring which version's copy matched."""
    return {doc["_original_id"] for doc in col.find_many(query)}


@pytest.mark.parametrize("how", INSERT_PATHS)
@pytest.mark.parametrize("later", [("country", "P", "Q"), ("city", "P", "Q")],
                         ids=["other_field", "same_field_other_value"])
def test_old_term_survives_an_unrelated_later_operation(make_collection, how, later):
    col = make_collection("preprocess")
    _insert(col, how, "A")
    _translate(col, "city", "A", "B", 1950)
    before = _found(col, {"city": "A"})
    assert len(before) == 1

    _translate(col, *later, 2000)

    assert _found(col, {"city": "A"}) == before


@pytest.mark.parametrize("how", INSERT_PATHS)
def test_old_term_finds_records_inserted_after_the_operation(make_collection, how):
    col = make_collection("preprocess")
    _translate(col, "city", "A", "B", 1950)
    _translate(col, "country", "P", "Q", 2000)
    _insert(col, how, "A")

    assert len(_found(col, {"city": "A"})) == 1


@pytest.mark.parametrize("how", INSERT_PATHS)
def test_every_term_of_a_chain_finds_the_record(make_collection, how):
    col = make_collection("preprocess")
    _insert(col, how, "A")
    _translate(col, "city", "A", "B", 1950)
    _translate(col, "city", "B", "C", 2000)
    _translate(col, "country", "P", "Q", 2010)

    for term in ("A", "B", "C"):
        assert len(_found(col, {"city": term})) == 1, term


def test_old_term_does_not_find_unrelated_values(make_collection):
    col = make_collection("preprocess")
    _insert(col, "bulk", "A")
    _insert(col, "bulk", "Moscow")
    moscow = _found(col, {"city": "Moscow"})
    _translate(col, "city", "A", "B", 1950)
    _translate(col, "country", "P", "Q", 2000)

    assert _found(col, {"city": "A"}).isdisjoint(moscow)
    assert _found(col, {"city": "Moscow"}) == moscow


def test_old_term_survives_an_operation_inserted_earlier_in_the_chain(make_collection):
    """The new operation takes over the previous version's place in the chain, and
    the older operation moves to the new version: its records must follow it."""
    col = make_collection("preprocess")
    _insert(col, "bulk", "A")
    _translate(col, "city", "A", "B", 2000)
    _translate(col, "country", "P", "Q", 1950)
    _translate(col, "country", "R", "S", 2010)

    assert len(_found(col, {"city": "A"})) == 1


@pytest.mark.parametrize("how", INSERT_PATHS)
def test_split_source_finds_its_fragments_after_a_later_operation(make_collection, how):
    col = make_collection("preprocess")
    _insert(col, how, "S1")
    fragment = _found(col, {"city": "S1"})
    col.execute_operation("splitting", datetime(1950, 1, 1),
                          {"fieldName": "city", "oldValue": "S", "newValues": ["S1", "S2"]})
    _translate(col, "country", "P", "Q", 2000)

    assert _found(col, {"city": "S"}) == fragment


def test_split_source_finds_fragments_inserted_after_the_operation(make_collection):
    single = make_collection("preprocess")
    bulk = make_collection("preprocess")
    for col, how in ((single, "insert_one"), (bulk, "bulk")):
        col.execute_operation("splitting", datetime(1950, 1, 1),
                              {"fieldName": "city", "oldValue": "S", "newValues": ["S1", "S2"]})
        _translate(col, "country", "P", "Q", 2000)
        _insert(col, how, "S1", valid_from=datetime(1960, 1, 1))
        assert len(_found(col, {"city": "S"})) == 1, how


def test_a_cascaded_reapplication_tags_the_records_it_creates(make_collection):
    """B->C registered first; A->B then makes the B->C evolution reprocess copies."""
    col = make_collection("preprocess")
    _insert(col, "bulk", "A")
    _translate(col, "city", "B", "C", 2000)
    _translate(col, "city", "A", "B", 1950)
    _translate(col, "country", "P", "Q", 2010)

    assert len(_found(col, {"city": "A"})) == 1
    assert len(_found(col, {"city": "C"})) == 1


@pytest.mark.parametrize("term", ["A", "B"])
def test_the_newest_operation_returns_each_record_once(make_collection, term):
    """Version ranges are half open: the two halves of a split must never both be
    visible, even when the split happened exactly at the current version."""
    col = make_collection("preprocess")
    _insert(col, "bulk", "A")
    _translate(col, "city", "A", "B", 1950)

    assert len(list(col.find_many({"city": term}))) == 1


def test_a_split_on_another_field_returns_each_record_once(make_collection):
    col = make_collection("preprocess")
    _insert(col, "bulk", "P", country="X")
    _translate(col, "country", "X", "Y", 1950)

    assert len(list(col.find_many({"city": "P"}))) == 1


def test_bulk_and_single_insert_store_the_same_ids(make_collection):
    single = make_collection("preprocess")
    bulk = make_collection("preprocess")
    for col, how in ((single, "insert_one"), (bulk, "bulk")):
        _translate(col, "city", "A", "B", 1950)
        _translate(col, "city", "B", "C", 2000)
        _insert(col, how, "A")

    def tags(col):
        rank = {v["_id"]: v["version_number"]
                for v in col.collection.collection_versions.find()}
        return sorted((doc["city"], sorted(rank[t] for t in doc["_evolution_list"]))
                      for doc in col.collection.collection_processed.find())

    assert tags(single) == tags(bulk)
