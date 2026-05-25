"""
Tests for TranslationOperation (1-to-1 reversible rename).

Naming convention
-----------------
*  insertion_first  – records are inserted first; the evolution is registered
   retroactively afterwards.
*  ops_first        – the evolution is registered first; records are inserted
   after.

Both the preprocess (eager, materialises semantic copies at write time) and
rewrite (lazy, expands queries at query time) strategies are covered.
"""
from datetime import datetime

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OLD = "Leningrad"
_NEW = "Saint Petersburg"
_UNRELATED = "Moscow"


# ===========================================================================
# PREPROCESS MODE
# ===========================================================================


class TestTranslationPreprocess:
    """BasicCollection in 'preprocess' mode."""

    # -----------------------------------------------------------------------
    # Forward evolution: record carries the OLD name before the rename
    # -----------------------------------------------------------------------

    def test_forward_insertion_first(self, make_collection, count):
        """Insert old name → register translation → both names return 1 record."""
        col = make_collection("preprocess")
        col.insert_one(
            '{"city": "Leningrad", "country": "Russia"}', datetime(1923, 12, 20)
        )
        col.execute_operation(
            "translation",
            datetime(1924, 1, 26),
            {"fieldName": "city", "oldValue": "Leningrad", "newValue": "Saint Petersburg"},
        )
        assert count(col, {"city": "Saint Petersburg"}) == 1
        assert count(col, {"city": "Leningrad"}) == 1

    def test_forward_insertion_first_multi_record(self, make_collection, count):
        """Multiple records with the old name: all become accessible via new name."""
        col = make_collection("preprocess")
        col.insert_one(
            '{"city": "Petrograd", "year": "1914"}', datetime(1914, 8, 31)
        )
        col.insert_one(
            '{"city": "Petrograd", "year": "1918"}', datetime(1918, 1, 1)
        )
        col.execute_operation(
            "translation",
            datetime(1924, 1, 26),
            {"fieldName": "city", "oldValue": "Petrograd", "newValue": "Leningrad"},
        )
        assert count(col, {"city": "Leningrad"}) == 2
        assert count(col, {"city": "Petrograd"}) == 2

    def test_forward_unrelated_record_not_affected(self, make_collection, count):
        """A record with an unrelated city value is unaffected by the translation."""
        col = make_collection("preprocess")
        col.insert_one(
            '{"city": "Moscow", "country": "Russia"}', datetime(1900, 1, 1)
        )
        col.insert_one(
            '{"city": "Leningrad", "country": "Russia"}', datetime(1923, 12, 20)
        )
        col.execute_operation(
            "translation",
            datetime(1924, 1, 26),
            {"fieldName": "city", "oldValue": "Leningrad", "newValue": "Saint Petersburg"},
        )
        assert count(col, {"city": "Moscow"}) == 1  # untouched

    # -----------------------------------------------------------------------
    # Backward evolution: record carries the NEW name; the rename is
    # registered retroactively (insertion_first pattern)
    # -----------------------------------------------------------------------

    def test_backward_insertion_first(self, make_collection, count):
        """Insert record with new name; register retroactive translation.
        Both the new and the historic old name should return 1 result."""
        col = make_collection("preprocess")
        col.insert_one(
            '{"city": "Ouro Preto", "country": "Brazil"}', datetime(2000, 12, 31)
        )
        col.execute_operation(
            "translation",
            datetime(1924, 1, 26),
            {"fieldName": "city", "oldValue": "Vila Rica", "newValue": "Ouro Preto"},
        )
        assert count(col, {"city": "Ouro Preto"}) == 1
        assert count(col, {"city": "Vila Rica"}) == 1

    # -----------------------------------------------------------------------
    # Ops-first backward: register evolution first, then insert a record with
    # the NEW name.  This path calls evolute_backward() — the typo regression.
    # -----------------------------------------------------------------------

    def test_ops_first_backward_no_crash(self, make_collection, count):
        """
        Regression test for the 'previous_opnext_versioneration.field' typo in
        TranslationOperation.evolute_backward() (line 260 before the fix).

        Scenario (ops-first):
          1. Register translation OldName → NewName.
          2. Insert a record with city='NewName'.
             → This triggers evolute_backward(); the typo caused a KeyError here.

        After the fix the insertion must complete without exception and the
        NewName record must be visible at the current version.
        """
        col = make_collection("preprocess")
        col.execute_operation(
            "translation",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValue": "OldName", "newValue": "NewName"},
        )
        # Before fix: KeyError: 'previous_opnext_versioneration.field'
        col.insert_one(
            '{"city": "NewName", "country": "X"}', datetime(2005, 1, 1)
        )
        # Post-fix: insert completes; NewName is visible at the current version
        assert count(col, {"city": "NewName"}) == 1

    # -----------------------------------------------------------------------
    # Multi-field records
    # -----------------------------------------------------------------------

    def test_only_translated_field_is_affected(self, make_collection, count):
        """Translation on 'city' must not change the 'country' field."""
        col = make_collection("preprocess")
        col.insert_one(
            '{"city": "OldCity", "country": "France"}', datetime(2000, 1, 1)
        )
        col.execute_operation(
            "translation",
            datetime(2005, 1, 1),
            {"fieldName": "city", "oldValue": "OldCity", "newValue": "NewCity"},
        )
        # City translation took effect
        assert count(col, {"city": "NewCity"}) >= 1
        # Country field is unchanged
        results = list(col.find_many({"city": "NewCity"}))
        assert any(r.get("country") == "France" for r in results)

    # -----------------------------------------------------------------------
    # Edge: no operations → direct query returns exactly what was inserted
    # -----------------------------------------------------------------------

    def test_no_evolution_direct_query(self, make_collection, count):
        """Without any evolution, a simple find returns the exact inserted record."""
        col = make_collection("preprocess")
        col.insert_one('{"city": "Rome", "country": "Italy"}', datetime(2000, 1, 1))
        assert count(col, {"city": "Rome"}) == 1
        assert count(col, {"city": "Berlin"}) == 0


# ===========================================================================
# REWRITE MODE
# ===========================================================================


class TestTranslationRewrite:
    """BasicCollection in 'rewrite' mode (lazy, expands queries at query time)."""

    # -----------------------------------------------------------------------
    # Forward
    # -----------------------------------------------------------------------

    def test_forward_insertion_first(self, make_collection):
        """Rewrite mode, insertion-first forward: both names return 1 result."""
        col = make_collection("rewrite")
        col.insert_one(
            '{"city": "Leningrad", "country": "Russia"}', datetime(1923, 12, 20)
        )
        col.execute_operation(
            "translation",
            datetime(1924, 1, 26),
            {"fieldName": "city", "oldValue": "Leningrad", "newValue": "Saint Petersburg"},
        )
        assert len(list(col.find_many({"city": "Saint Petersburg"}))) == 1
        assert len(list(col.find_many({"city": "Leningrad"}))) == 1

    def test_forward_ops_first(self, make_collection):
        """Rewrite mode, ops-first forward: register translation, insert old name."""
        col = make_collection("rewrite")
        col.execute_operation(
            "translation",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValue": "OldCity", "newValue": "NewCity"},
        )
        # Insert a record with the old name valid *before* the translation date
        col.insert_one(
            '{"city": "OldCity", "country": "X"}', datetime(1999, 1, 1)
        )
        assert len(list(col.find_many({"city": "OldCity"}))) == 1
        assert len(list(col.find_many({"city": "NewCity"}))) == 1

    def test_backward_ops_first(self, make_collection):
        """Rewrite mode, ops-first backward: register translation, insert new name."""
        col = make_collection("rewrite")
        col.execute_operation(
            "translation",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValue": "OldCity", "newValue": "NewCity"},
        )
        # Insert a record with the new name (post-evolution date)
        col.insert_one(
            '{"city": "NewCity", "country": "X"}', datetime(2005, 1, 1)
        )
        assert len(list(col.find_many({"city": "OldCity"}))) == 1
        assert len(list(col.find_many({"city": "NewCity"}))) == 1

    # -----------------------------------------------------------------------
    # Backward insertion-first
    # -----------------------------------------------------------------------

    def test_backward_insertion_first(self, make_collection):
        """Rewrite mode, insertion-first backward: both names return 1 result."""
        col = make_collection("rewrite")
        col.insert_one(
            '{"city": "Ouro Preto", "country": "Brazil"}', datetime(2000, 12, 31)
        )
        col.execute_operation(
            "translation",
            datetime(1924, 1, 26),
            {"fieldName": "city", "oldValue": "Vila Rica", "newValue": "Ouro Preto"},
        )
        assert len(list(col.find_many({"city": "Ouro Preto"}))) == 1
        assert len(list(col.find_many({"city": "Vila Rica"}))) == 1
