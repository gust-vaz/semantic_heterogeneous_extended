"""
Tests that concurrent `execute_operation` calls leave the version chain in a
valid doubly-linked state with no duplicate version numbers.
"""
import concurrent.futures
from datetime import datetime

import pytest

from tests.distributed.conftest import assert_chain_integrity


def _register_translation(col, valid_from, old_value, new_value):
    """Helper: register a translation and return any exception."""
    try:
        col.execute_operation(
            "translation",
            valid_from,
            {"fieldName": "city", "oldValue": old_value, "newValue": new_value}
        )
        return None
    except Exception as e:
        return e


class TestConcurrentOperations:

    def test_two_concurrent_translations_chain_integrity(self, make_rs_collection):
        """
        Two translations registered simultaneously must leave the chain consistent.
        Both operations target different date ranges so they must produce two
        distinct nodes in the chain.
        """
        col = make_rs_collection("preprocess", "majority")
        col.insert_one('{"city": "A", "pop": 1}', datetime(1990, 1, 1))

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            f1 = executor.submit(
                _register_translation, col, datetime(2000, 1, 1), "A", "B"
            )
            f2 = executor.submit(
                _register_translation, col, datetime(2010, 1, 1), "B", "C"
            )
            e1 = f1.result()
            e2 = f2.result()

        # One or both may succeed; neither may silently corrupt the chain
        assert_chain_integrity(col.collection.collection_versions)

        all_versions = list(col.collection.collection_versions.find())
        version_numbers = [v['version_number'] for v in all_versions]
        assert len(version_numbers) == len(set(version_numbers)), (
            "Duplicate version_number values detected after concurrent inserts"
        )

    def test_five_concurrent_translations_chain_integrity(self, make_rs_collection):
        """
        Five translations on five different date ranges all committed concurrently.
        Chain must remain consistent afterward.
        """
        col = make_rs_collection("preprocess", "majority")
        col.insert_one('{"city": "Start", "pop": 1}', datetime(1980, 1, 1))

        tasks = [
            (datetime(1990 + i * 5, 1, 1), f"V{i}", f"V{i+1}")
            for i in range(5)
        ]

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(_register_translation, col, d, old, new)
                for d, old, new in tasks
            ]
            errors = [f.result() for f in futures]

        assert_chain_integrity(col.collection.collection_versions)

    def test_concurrent_insert_and_evolve_no_missing_records(self, make_rs_collection):
        """
        Inserting records on one thread while registering an evolution on another.
        After both complete, querying for the evolved name returns at least one record.
        """
        col = make_rs_collection("preprocess", "majority")
        # Pre-insert some records before the concurrent phase
        for i in range(5):
            col.insert_one(f'{{"city": "ConcBase", "idx": {i}}}', datetime(1990, 1, 1))

        results_before = list(col.find_many({"city": "ConcBase"}))

        def insert_more():
            for i in range(5, 10):
                col.insert_one(f'{{"city": "ConcBase", "idx": {i}}}', datetime(1990, 1, 1))

        def register_translation():
            col.execute_operation(
                "translation",
                datetime(2000, 1, 1),
                {"fieldName": "city", "oldValue": "ConcBase", "newValue": "ConcNew"}
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            f_insert = executor.submit(insert_more)
            f_evolve = executor.submit(register_translation)
            f_insert.result()
            f_evolve.result()

        assert_chain_integrity(col.collection.collection_versions)

        results_after = list(col.find_many({"city": "ConcNew"}))
        assert len(results_after) >= 5, (
            f"Expected at least 5 records for ConcNew, got {len(results_after)}"
        )
