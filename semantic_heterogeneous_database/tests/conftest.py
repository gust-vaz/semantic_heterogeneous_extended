"""
pytest fixtures for MellowDB tests.

Each test gets its own isolated MongoDB database (UUID-named) that is
automatically dropped after the test completes. MongoDB connection is
configured via the MONGO_HOST environment variable (default: localhost)
so the same tests run both locally and inside the Docker runner container.

Setting MELLOW_SHARD_KEY to a JSON key pattern (for example
'{"municipio": "hashed"}') makes every collection this suite builds a sharded
one, which turns the whole suite into a correctness suite for sharding without
changing a single test.
"""
import json
import os
import uuid
import pytest

from semantic_heterogeneous_database import BasicCollection

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb://localhost:27017/?directConnection=true")

_SHARD_KEY_ENV = os.environ.get("MELLOW_SHARD_KEY")
DEFAULT_SHARD_KEY = json.loads(_SHARD_KEY_ENV) if _SHARD_KEY_ENV else None

#: Distinguishes "caller said nothing" from "caller explicitly said no shard key".
#: Without it, a test that needs an unsharded collection could not get one while
#: MELLOW_SHARD_KEY is set, because None would be indistinguishable from absent.
_UNSET = object()


@pytest.fixture
def make_collection():
    """
    Factory fixture that creates fresh BasicCollection instances with
    auto-generated database names for test isolation.

    Usage::

        def test_something(make_collection):
            col = make_collection('preprocess')   # or 'rewrite'
            col.insert_one(...)

    Omitting `shard_key` uses whatever MELLOW_SHARD_KEY asked for. Passing it
    explicitly overrides that for one collection, including `shard_key=None`,
    which forces an unsharded collection even under a sharded run.
    """
    created: list[BasicCollection] = []

    def _make(mode: str = "preprocess", shard_key=_UNSET) -> BasicCollection:
        db_name = f"mellowtest_{uuid.uuid4().hex[:12]}"
        key = DEFAULT_SHARD_KEY if shard_key is _UNSET else shard_key
        col = BasicCollection(db_name, "col", MONGO_HOST, mode, shard_key=key)
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
