"""
Tests for GroupingOperation (many-to-one merge).

Terminology
-----------
* oldValues  – the names that existed *before* the merge (G, H)
* newValue   – the single name that replaces them after the merge (K)
* forward    – a record carries an old value; it gets a processed copy with
               the new value.
* backward   – a record carries the new value; it represents post-merge data.
               Querying an old value should still find it (via query expansion).
"""
from datetime import datetime

import pytest


# ===========================================================================
# PREPROCESS MODE
# ===========================================================================


class TestGroupingPreprocess:
    """BasicCollection in 'preprocess' mode."""

    # -----------------------------------------------------------------------
    # Forward: records inserted with old names before the merge
    # -----------------------------------------------------------------------

    def test_forward_insertion_first_new_name_count(self, make_collection, count):
        """After merging G and H into K, querying K returns 2 (one per source)."""
        col = make_collection("preprocess")
        col.insert_one('{"city": "G", "country": "F"}', datetime(1900, 12, 20))
        col.insert_one('{"city": "H", "country": "F"}', datetime(1910, 12, 20))
        col.execute_operation(
            "grouping",
            datetime(1930, 6, 20),
            {"fieldName": "city", "oldValues": ["G", "H"], "newValue": "K"},
        )
        # Two source records → two K-labelled processed records
        assert count(col, {"city": "K"}) == 2

    def test_forward_insertion_first_three_sources(self, make_collection, count):
        """Merging three cities into one: querying the merged name returns 3."""
        col = make_collection("preprocess")
        for city in ["X1", "X2", "X3"]:
            col.insert_one(f'{{"city": "{city}", "country": "F"}}', datetime(1900, 1, 1))
        col.execute_operation(
            "grouping",
            datetime(1950, 1, 1),
            {"fieldName": "city", "oldValues": ["X1", "X2", "X3"], "newValue": "Xmerged"},
        )
        assert count(col, {"city": "Xmerged"}) == 3

    # -----------------------------------------------------------------------
    # Record whose city is the *merged* value but predates the grouping
    # -----------------------------------------------------------------------

    def test_record_predates_grouping(self, make_collection, count):
        """
        A record with city='J' is inserted BEFORE the grouping Je1,Je2→J.
        It is an independent entity called J that predates the merge — it
        should remain exactly as-is and querying Je1 should return 0.
        """
        col = make_collection("preprocess")
        col.insert_one('{"city": "J", "country": "E"}', datetime(1950, 4, 5))
        col.execute_operation(
            "grouping",
            datetime(2009, 11, 20),
            {"fieldName": "city", "oldValues": ["Je1", "Je2"], "newValue": "J"},
        )
        assert count(col, {"city": "J"}) == 1
        assert count(col, {"city": "Je1"}) == 0

    # -----------------------------------------------------------------------
    # Backward: record already carries the merged value
    # -----------------------------------------------------------------------

    def test_backward_insertion_first_new_name_visible(self, make_collection, count):
        """
        Insert a record with city='Z' (the merged value).
        Register grouping Y,U→Z.
        Querying Z must still return 1 result.
        """
        col = make_collection("preprocess")
        col.insert_one('{"city": "Z", "country": "F"}', datetime(2000, 6, 14))
        col.execute_operation(
            "grouping",
            datetime(1950, 11, 20),
            {"fieldName": "city", "oldValues": ["Y", "U"], "newValue": "Z"},
        )
        assert count(col, {"city": "Z"}) == 1

    # -----------------------------------------------------------------------
    # Verify grouping also works with the new alias 'merging'
    # -----------------------------------------------------------------------

    def test_merging_alias(self, make_collection, count):
        """The operation type 'merging' is an accepted alias for 'grouping'."""
        col = make_collection("preprocess")
        col.insert_one('{"city": "Alpha", "country": "X"}', datetime(1900, 1, 1))
        col.insert_one('{"city": "Beta", "country": "X"}', datetime(1910, 1, 1))
        col.execute_operation(
            "merging",
            datetime(1950, 1, 1),
            {"fieldName": "city", "oldValues": ["Alpha", "Beta"], "newValue": "Gamma"},
        )
        assert count(col, {"city": "Gamma"}) == 2


# ===========================================================================
# REWRITE MODE
# ===========================================================================


class TestGroupingRewrite:
    """BasicCollection in 'rewrite' mode."""

    def test_forward_insertion_first(self, make_collection):
        """Rewrite mode: merge G,H→K; querying K via find_many returns 2."""
        col = make_collection("rewrite")
        col.insert_one('{"city": "G", "country": "F"}', datetime(1900, 12, 20))
        col.insert_one('{"city": "H", "country": "F"}', datetime(1910, 12, 20))
        col.execute_operation(
            "grouping",
            datetime(1930, 6, 20),
            {"fieldName": "city", "oldValues": ["G", "H"], "newValue": "K"},
        )
        assert len(list(col.find_many({"city": "K"}))) == 2

    def test_record_predates_grouping(self, make_collection):
        """Rewrite mode: pre-existing merged-value record is untouched."""
        col = make_collection("rewrite")
        col.insert_one('{"city": "J", "country": "E"}', datetime(1950, 4, 5))
        col.execute_operation(
            "grouping",
            datetime(2009, 11, 20),
            {"fieldName": "city", "oldValues": ["Je1", "Je2"], "newValue": "J"},
        )
        assert len(list(col.find_many({"city": "J"}))) == 1
        assert len(list(col.find_many({"city": "Je1"}))) == 0
