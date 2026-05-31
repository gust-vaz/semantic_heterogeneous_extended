"""
Fixtures for distributed (replica set) tests.

These tests require a running 3-node MongoDB replica set. They are skipped
automatically when the MONGO_RS_URI environment variable is not set or when
the connected MongoDB is not a replica set.
"""
import os
import uuid
import pytest
from pymongo import MongoClient
from semantic_heterogeneous_database import BasicCollection

MONGO_RS_URI = os.environ.get(
    "MONGO_RS_URI",
    "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
)


def _mongo_is_replica_set(uri: str) -> bool:
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=3000)
        info = client.admin.command('hello')
        client.close()
        return bool(info.get('setName'))
    except Exception:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_replica_set():
    """Skip the entire distributed test module if no replica set is available."""
    if not _mongo_is_replica_set(MONGO_RS_URI):
        pytest.skip(
            "No MongoDB replica set found at MONGO_RS_URI. "
            "Start docker compose -f docker-compose.replicaset.yml up before running these tests.",
            allow_module_level=True
        )


@pytest.fixture
def rs_uri() -> str:
    return MONGO_RS_URI


@pytest.fixture
def make_rs_collection(rs_uri):
    """
    Factory that creates a fresh BasicCollection connected to the replica set.
    Each collection gets a UUID-named database for isolation, dropped after the test.
    """
    created: list[BasicCollection] = []

    def _make(mode: str = "preprocess", write_concern: str = "majority") -> BasicCollection:
        db_name = f"mellowtest_rs_{uuid.uuid4().hex[:12]}"
        col = BasicCollection(db_name, "col", rs_uri, mode, write_concern)
        created.append(col)
        return col

    yield _make

    for col in created:
        try:
            col.collection.client.drop_database(col.database_name)
        except Exception:
            pass


@pytest.fixture
def rs_client(rs_uri) -> MongoClient:
    """Raw MongoClient connected to the replica set."""
    client = MongoClient(rs_uri)
    yield client
    client.close()


def assert_chain_integrity(versions_collection):
    """
    Traverse the version doubly-linked list and assert it is consistent.
    - Every node whose `next_version` is not None has a successor whose
      `previous_version` equals this node's `version_number`.
    - No duplicate `version_number` values.
    - Exactly one node has `current_version == 1`.
    """
    docs = list(versions_collection.find())
    by_number = {d['version_number']: d for d in docs}

    assert len(by_number) == len(docs), "Duplicate version_number values found"

    current_count = sum(1 for d in docs if d.get('current_version') == 1)
    assert current_count == 1, f"Expected exactly one current version, found {current_count}"

    for doc in docs:
        next_vn = doc.get('next_version')
        if next_vn is not None:
            assert next_vn in by_number, f"next_version {next_vn} has no matching node"
            successor = by_number[next_vn]
            assert successor['previous_version'] == doc['version_number'], (
                f"Broken chain: {doc['version_number']}.next={next_vn} "
                f"but {next_vn}.previous={successor['previous_version']}"
            )

        prev_vn = doc.get('previous_version')
        if prev_vn is not None:
            assert prev_vn in by_number, f"previous_version {prev_vn} has no matching node"
            predecessor = by_number[prev_vn]
            assert predecessor['next_version'] == doc['version_number'], (
                f"Broken chain: {doc['version_number']}.previous={prev_vn} "
                f"but {prev_vn}.next={predecessor['next_version']}"
            )
