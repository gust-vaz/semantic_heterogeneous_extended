"""
pytest fixtures for MellowDB tests.

Each test gets its own isolated MongoDB database (UUID-named) that is
automatically dropped after the test completes. MongoDB connection is
configured via the MONGO_HOST environment variable (default: localhost)
so the same tests run both locally and inside the Docker runner container.
"""
import os
import uuid
import pytest

from semantic_heterogeneous_database import BasicCollection

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb://localhost:27017/?directConnection=true")


@pytest.fixture
def make_collection():
    """
    Factory fixture that creates fresh BasicCollection instances with
    auto-generated database names for test isolation.

    Usage::

        def test_something(make_collection):
            col = make_collection('preprocess')   # or 'rewrite'
            col.insert_one(...)
    """
    created: list[BasicCollection] = []

    def _make(mode: str = "preprocess") -> BasicCollection:
        db_name = f"mellowtest_{uuid.uuid4().hex[:12]}"
        col = BasicCollection(db_name, "col", MONGO_HOST, mode)
        created.append(col)
        return col

    yield _make

    # Teardown: drop every database created during this test
    for col in created:
        try:
            col.collection.client.drop_database(col.database_name)
        except Exception:
            pass


@pytest.fixture
def count():
    """
    Counting helper that works across both operation modes.

    * preprocess mode → col.count_documents (queries the processed collection)
    * rewrite mode    → len(list(col.find_many(...))) (queries the raw collection)

    Usage::

        def test_something(make_collection, count):
            col = make_collection('preprocess')
            ...
            assert count(col, {'city': 'X'}) == 1
    """

    def _count(col: BasicCollection, query: dict) -> int:
        if col.operation_mode == "preprocess":
            return col.count_documents(query)
        return len(list(col.find_many(query)))

    return _count
