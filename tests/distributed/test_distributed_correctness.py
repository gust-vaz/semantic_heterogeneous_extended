"""
Tests that the `readConcern=majority` + `ReadPreference.PRIMARY` guarantee on
`_versions_r` produces semantically correct query results on a replica set.
"""
from datetime import datetime
import pytest
from pymongo import ReadPreference
from pymongo.read_concern import ReadConcern

from tests.distributed.conftest import assert_chain_integrity


class TestLinearizableVersionReads:

    def test_translation_query_correct_after_registration(self, make_rs_collection, rs_client):
        """
        Registering a translation and immediately querying returns the correct
        records from both before and after the rename, with correct counts.
        """
        col = make_rs_collection("preprocess", "majority")
        col.insert_one('{"city": "OldName", "pop": 100}', datetime(2000, 1, 1))
        col.insert_one('{"city": "NewName", "pop": 200}', datetime(2005, 1, 1))

        col.execute_operation(
            "translation",
            datetime(2003, 6, 1),
            {"fieldName": "city", "oldValue": "OldName", "newValue": "NewName"}
        )

        # Querying "NewName" must return BOTH records (pre- and post-rename)
        results = list(col.find_many({"city": "NewName"}))
        assert len(results) == 2, (
            f"Expected 2 results for 'NewName' after translation, got {len(results)}"
        )

    def test_versions_collection_read_uses_primary(self, make_rs_collection):
        """
        `_versions_r` always routes reads to the PRIMARY with majority concern.
        """
        col = make_rs_collection("preprocess", "majority")
        collection = col.collection  # the Collection object

        versions_r = collection._versions_r
        assert versions_r.read_preference == ReadPreference.PRIMARY, (
            f"Expected PRIMARY read preference, got {versions_r.read_preference}"
        )
        assert versions_r.read_concern.document == {'level': 'majority'}, (
            f"Expected majority read concern, got {versions_r.read_concern}"
        )

    def test_chain_integrity_after_multiple_operations(self, make_rs_collection):
        """
        After registering multiple operations, the version chain is consistent.
        """
        col = make_rs_collection("preprocess", "majority")
        col.insert_one('{"city": "A", "region": "X"}', datetime(1990, 1, 1))

        col.execute_operation(
            "translation",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValue": "A", "newValue": "B"}
        )
        col.execute_operation(
            "translation",
            datetime(2010, 1, 1),
            {"fieldName": "city", "oldValue": "B", "newValue": "C"}
        )

        assert_chain_integrity(col.collection.collection_versions)

    def test_grouping_query_correct_after_registration(self, make_rs_collection):
        """
        After a grouping operation, querying the merged name returns all source records.
        """
        col = make_rs_collection("preprocess", "majority")
        col.insert_one('{"city": "G1", "pop": 50}', datetime(1990, 1, 1))
        col.insert_one('{"city": "G2", "pop": 60}', datetime(1990, 1, 1))

        col.execute_operation(
            "grouping",
            datetime(2000, 1, 1),
            {"fieldName": "city", "oldValues": ["G1", "G2"], "newValue": "Merged"}
        )

        results = list(col.find_many({"city": "Merged"}))
        assert len(results) == 2

    def test_rewrite_mode_translation_correct(self, make_rs_collection):
        """
        Query rewriting mode produces semantically correct results on replica set.
        """
        col = make_rs_collection("rewrite", "majority")
        col.insert_one('{"city": "Leningrad", "year": 1980}', datetime(1980, 1, 1))
        col.insert_one('{"city": "Saint Petersburg", "year": 2000}', datetime(2000, 1, 1))

        col.execute_operation(
            "translation",
            datetime(1991, 9, 6),
            {"fieldName": "city", "oldValue": "Leningrad", "newValue": "Saint Petersburg"}
        )

        results = list(col.find_many({"city": "Saint Petersburg"}))
        assert len(results) == 2, (
            f"Rewrite mode: expected 2 results for Saint Petersburg, got {len(results)}"
        )
