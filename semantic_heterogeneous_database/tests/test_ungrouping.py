"""
Tests for UngroupingOperation (one-to-many split / splitting).

Terminology
-----------
* oldValue   – the single name that existed *before* the split (Pi)
* newValues  – the list of names after the split  ([Q, W])
* forward    – record carries the old value (Pi); on query the new names should
               resolve back to it (forward_processable = True).
* backward   – records carry new names (Q, W); they were inserted after the
               split and need a backward copy representing the pre-split name.
"""
from datetime import datetime

import pytest


# ===========================================================================
# PREPROCESS MODE
# ===========================================================================


class TestUngroupingPreprocess:
    """BasicCollection in 'preprocess' mode."""

    # -----------------------------------------------------------------------
    # Forward: record carries the old (pre-split) value
    # -----------------------------------------------------------------------

    def test_forward_insertion_first_old_name_visible(self, make_collection, count):
        """
        Insert Pi before the split Pi→[Q,W].
        The Pi record must still be visible via count(Pi).
        """
        col = make_collection("preprocess")
        col.insert_one('{"city": "Pi", "country": "X"}', datetime(1900, 12, 20))
        col.execute_operation(
            "ungrouping",
            datetime(1930, 6, 20),
            {"fieldName": "city", "oldValue": "Pi", "newValues": ["Q", "W"]},
        )
        assert count(col, {"city": "Pi"}) == 1

    # -----------------------------------------------------------------------
    # Backward: records carry the new (post-split) values; querying the old
    # value should find them
    # -----------------------------------------------------------------------

    def test_backward_insertion_first(self, make_collection, count):
        """
        Insert Gr1 and Gr2 (new names), then register split U→[Gr1, Gr2].
        Querying U (old name) should return 2 (one from each new-name record).
        """
        col = make_collection("preprocess")
        col.insert_one('{"city": "Gr1", "country": "C"}', datetime(2000, 6, 14))
        col.insert_one('{"city": "Gr2", "country": "C"}', datetime(2000, 6, 14))
        col.execute_operation(
            "ungrouping",
            datetime(1950, 11, 20),
            {"fieldName": "city", "oldValue": "U", "newValues": ["Gr1", "Gr2"]},
        )
        assert count(col, {"city": "U"}) == 2
        assert count(col, {"city": "Gr1"}) == 1
        assert count(col, {"city": "Gr2"}) == 1

    def test_backward_insertion_first_single_record(self, make_collection, count):
        """Single post-split record: querying the old value returns 1."""
        col = make_collection("preprocess")
        col.insert_one('{"city": "NewA", "country": "Z"}', datetime(2005, 1, 1))
        col.execute_operation(
            "ungrouping",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValue": "OldPi", "newValues": ["NewA", "NewB"]},
        )
        assert count(col, {"city": "OldPi"}) == 1
        assert count(col, {"city": "NewA"}) == 1

    # -----------------------------------------------------------------------
    # Record predates the split
    # -----------------------------------------------------------------------

    def test_record_predates_split(self, make_collection, count):
        """
        A record with city='Hg1' is inserted BEFORE the split Pa1→[Hg1, Hg2].
        The backward section creates a Pa1 copy for the pre-split era.
        Querying Hg1 returns 1; querying Pa1 also returns 1 (the backward copy).
        """
        col = make_collection("preprocess")
        col.insert_one('{"city": "Hg1", "country": "F"}', datetime(1950, 4, 5))
        col.execute_operation(
            "ungrouping",
            datetime(2009, 11, 20),
            {"fieldName": "city", "oldValue": "Pa1", "newValues": ["Hg1", "Hg2"]},
        )
        assert count(col, {"city": "Hg1"}) == 1
        # Pa1 backward copy represents the Hg1 record in the pre-split era
        assert count(col, {"city": "Pa1"}) == 1

    # -----------------------------------------------------------------------
    # Alias 'splitting'
    # -----------------------------------------------------------------------

    def test_splitting_alias(self, make_collection, count):
        """The operation type 'splitting' is an accepted alias for 'ungrouping'."""
        col = make_collection("preprocess")
        col.insert_one('{"city": "S1", "country": "X"}', datetime(2000, 1, 1))
        col.insert_one('{"city": "S2", "country": "X"}', datetime(2000, 1, 1))
        col.execute_operation(
            "splitting",
            datetime(1990, 1, 1),
            {"fieldName": "city", "oldValue": "S0", "newValues": ["S1", "S2"]},
        )
        assert count(col, {"city": "S0"}) == 2


# ===========================================================================
# REWRITE MODE
# ===========================================================================


class TestUngroupingRewrite:
    """BasicCollection in 'rewrite' mode."""

    def test_backward_insertion_first(self, make_collection):
        """Rewrite mode: split U→[Gr1,Gr2]; querying U returns 2."""
        col = make_collection("rewrite")
        col.insert_one('{"city": "Gr1", "country": "C"}', datetime(2000, 6, 14))
        col.insert_one('{"city": "Gr2", "country": "C"}', datetime(2000, 6, 14))
        col.execute_operation(
            "ungrouping",
            datetime(1950, 11, 20),
            {"fieldName": "city", "oldValue": "U", "newValues": ["Gr1", "Gr2"]},
        )
        assert len(list(col.find_many({"city": "U"}))) == 2
        assert len(list(col.find_many({"city": "Gr1"}))) == 1
