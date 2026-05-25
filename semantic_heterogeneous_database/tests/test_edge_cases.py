"""
Edge-case and defensive tests for MellowDB.

These tests exercise boundary conditions, invalid inputs, and behaviours that
are easy to break during refactoring:
  - empty collections
  - evolutions registered on empty databases
  - multi-record consistency
  - invalid operation mode
  - create_index doesn't break queries
  - preprocess vs rewrite mode give consistent counts for simple queries
"""
from datetime import datetime

import pytest


class TestEmptyCollection:
    def test_query_empty_preprocess(self, make_collection, count):
        """Querying a collection with no records returns 0."""
        col = make_collection("preprocess")
        assert count(col, {"city": "Anything"}) == 0

    def test_query_empty_rewrite(self, make_collection):
        """Rewrite mode: querying a collection with no records returns 0."""
        col = make_collection("rewrite")
        assert len(list(col.find_many({"city": "Anything"}))) == 0

    def test_evolution_on_empty_collection_preprocess(self, make_collection, count):
        """Registering an evolution on an empty database must not crash."""
        col = make_collection("preprocess")
        col.execute_operation(
            "translation",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValue": "OldName", "newValue": "NewName"},
        )
        assert count(col, {"city": "OldName"}) == 0
        assert count(col, {"city": "NewName"}) == 0

    def test_evolution_on_empty_collection_rewrite(self, make_collection):
        """Rewrite mode: evolution on empty database must not crash."""
        col = make_collection("rewrite")
        col.execute_operation(
            "translation",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValue": "OldName", "newValue": "NewName"},
        )
        assert len(list(col.find_many({"city": "OldName"}))) == 0
        assert len(list(col.find_many({"city": "NewName"}))) == 0


class TestInvalidInput:
    def test_invalid_operation_mode_raises(self):
        """Passing an unrecognised operation_mode must raise."""
        import uuid
        from semantic_heterogeneous_database import BasicCollection
        import os

        host = os.environ.get("MONGO_HOST", "localhost")
        db_name = f"mellowtest_{uuid.uuid4().hex[:8]}"
        with pytest.raises(BaseException):
            col = BasicCollection(db_name, "col", host, "invalid_mode")
            # cleanup if no error (shouldn't happen, but be safe)
            col.collection.client.drop_database(db_name)

    def test_missing_fieldname_raises(self, make_collection):
        """execute_operation without 'fieldName' must raise."""
        col = make_collection("preprocess")
        with pytest.raises((BaseException, Exception)):
            col.execute_operation(
                "translation",
                datetime(2000, 1, 1),
                {"oldValue": "A", "newValue": "B"},  # no fieldName
            )

    def test_grouping_missing_oldvalues_raises(self, make_collection):
        """Grouping without 'oldValues' must raise."""
        col = make_collection("preprocess")
        with pytest.raises((BaseException, Exception)):
            col.execute_operation(
                "grouping",
                datetime(2000, 1, 1),
                {"fieldName": "city", "newValue": "K"},  # no oldValues
            )

    def test_ungrouping_missing_newvalues_raises(self, make_collection):
        """Ungrouping without 'newValues' must raise."""
        col = make_collection("preprocess")
        with pytest.raises((BaseException, Exception)):
            col.execute_operation(
                "ungrouping",
                datetime(2000, 1, 1),
                {"fieldName": "city", "oldValue": "Pi"},  # no newValues
            )


class TestMultipleRecords:
    def test_five_records_all_translated(self, make_collection, count):
        """All 5 records with the old name become accessible via the new name."""
        col = make_collection("preprocess")
        for i in range(5):
            col.insert_one(
                f'{{"city": "SmallTown", "id": {i}}}', datetime(2000, 1, 1)
            )
        col.execute_operation(
            "translation",
            datetime(2010, 1, 1),
            {"fieldName": "city", "oldValue": "SmallTown", "newValue": "BigCity"},
        )
        assert count(col, {"city": "BigCity"}) == 5
        assert count(col, {"city": "SmallTown"}) == 5

    def test_mixed_records_only_matching_translated(self, make_collection, count):
        """Only records with the translated city are affected; others are untouched."""
        col = make_collection("preprocess")
        col.insert_one('{"city": "TargetCity", "country": "A"}', datetime(2000, 1, 1))
        col.insert_one('{"city": "OtherCity", "country": "B"}', datetime(2000, 1, 1))
        col.execute_operation(
            "translation",
            datetime(2005, 1, 1),
            {"fieldName": "city", "oldValue": "TargetCity", "newValue": "RenamedCity"},
        )
        assert count(col, {"city": "RenamedCity"}) == 1
        assert count(col, {"city": "OtherCity"}) == 1  # untouched
        assert count(col, {"city": "TargetCity"}) == 1  # still queryable


class TestIndexCreation:
    def test_create_index_does_not_break_queries_preprocess(
        self, make_collection, count
    ):
        """Creating an index after insertions must not affect query correctness."""
        col = make_collection("preprocess")
        col.insert_one('{"city": "Rome", "country": "Italy"}', datetime(2000, 1, 1))
        col.execute_operation(
            "translation",
            datetime(2005, 1, 1),
            {"fieldName": "city", "oldValue": "Roma", "newValue": "Rome"},
        )
        col.create_index(["city"])
        # Index creation must not change query results
        assert count(col, {"city": "Rome"}) == 1

    def test_create_index_does_not_break_queries_rewrite(self, make_collection):
        """Rewrite mode: create_index must not affect query correctness."""
        col = make_collection("rewrite")
        col.insert_one('{"city": "Rome", "country": "Italy"}', datetime(2000, 1, 1))
        col.execute_operation(
            "translation",
            datetime(2005, 1, 1),
            {"fieldName": "city", "oldValue": "Roma", "newValue": "Rome"},
        )
        col.create_index(["city"])
        assert len(list(col.find_many({"city": "Rome"}))) == 1


class TestModeConsistency:
    def test_simple_find_same_result_both_modes(self, make_collection, count):
        """
        For a simple insertion (no evolution), both modes should agree on the
        count of records for a direct field query.
        """
        pre = make_collection("preprocess")
        rew = make_collection("rewrite")

        for col in [pre, rew]:
            col.insert_one('{"city": "Athens", "country": "Greece"}', datetime(2000, 1, 1))

        pre_count = count(pre, {"city": "Athens"})
        rew_count = len(list(rew.find_many({"city": "Athens"})))
        assert pre_count == rew_count == 1

    def test_translated_field_same_count_both_modes(self, make_collection, count):
        """
        After a forward translation, both modes should return the same number
        of records for a query on the new name.
        """
        pre = make_collection("preprocess")
        rew = make_collection("rewrite")

        for col in [pre, rew]:
            col.insert_one(
                '{"city": "OldName", "country": "X"}', datetime(2000, 1, 1)
            )
            col.execute_operation(
                "translation",
                datetime(2010, 1, 1),
                {"fieldName": "city", "oldValue": "OldName", "newValue": "NewName"},
            )

        pre_new = count(pre, {"city": "NewName"})
        rew_new = len(list(rew.find_many({"city": "NewName"})))
        assert pre_new == rew_new == 1

        pre_old = count(pre, {"city": "OldName"})
        rew_old = len(list(rew.find_many({"city": "OldName"})))
        assert pre_old == rew_old == 1
