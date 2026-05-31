"""
Failover tests: verify the system survives a Primary failure and subsequent
election. Requires Docker SDK and the replica set running.

These tests are slow (~30-60s each) due to MongoDB election time.
Run explicitly: pytest tests/distributed/test_failover.py -m slow -v
"""
import time
from datetime import datetime

import pytest

try:
    import docker as docker_sdk
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False

from tests.distributed.conftest import assert_chain_integrity

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def docker_client():
    if not DOCKER_AVAILABLE:
        pytest.skip("docker Python package not installed")
    try:
        client = docker_sdk.from_env()
        client.ping()
        return client
    except Exception:
        pytest.skip("Docker daemon not accessible")


def _get_primary_container(docker_client_):
    """Find the running primary container by name."""
    for name in ["semantic_heterogeneous_extended-mongo-primary-1",
                 "semantic-heterogeneous-extended-mongo-primary-1",
                 "mongo-primary"]:
        try:
            container = docker_client_.containers.get(name)
            if container.status == 'running':
                return container
        except Exception:
            pass
    pytest.skip("Cannot locate running mongo-primary container — is the replica set running?")


def _wait_for_primary(rs_client_, timeout=60):
    """Wait until the replica set has an elected PRIMARY."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = rs_client_.admin.command('replSetGetStatus')
            for member in status['members']:
                if member['stateStr'] == 'PRIMARY':
                    return True
        except Exception:
            pass
        time.sleep(2)
    return False


def _get_rs_client_for_new_primary(timeout=60):
    """Return a MongoClient that connects to whichever node is now the PRIMARY."""
    from pymongo import MongoClient
    deadline = time.time() + timeout
    while time.time() < deadline:
        for port in [27017, 27018, 27019]:
            try:
                c = MongoClient(f"mongodb://localhost:{port}/?directConnection=true",
                                serverSelectionTimeoutMS=2000)
                info = c.admin.command('hello')
                if info.get('isWritablePrimary') or info.get('ismaster'):
                    return c
                c.close()
            except Exception:
                pass
        time.sleep(2)
    return None


class TestFailover:

    def test_operation_before_failover_survives(
        self, make_rs_collection, rs_client, docker_client
    ):
        """
        Register a translation; stop the Primary; wait for election;
        query the new Primary — result is correct.
        """
        col = make_rs_collection("preprocess", "majority")
        col.insert_one('{"city": "PreFail", "pop": 10}', datetime(2000, 1, 1))
        col.execute_operation(
            "translation",
            datetime(2005, 1, 1),
            {"fieldName": "city", "oldValue": "PreFail", "newValue": "PostFail"}
        )

        primary_container = _get_primary_container(docker_client)
        primary_container.stop()

        try:
            # Find the new primary
            new_client = _get_rs_client_for_new_primary(timeout=60)
            assert new_client is not None, "No new PRIMARY elected within 60s"

            # Query on new primary
            from semantic_heterogeneous_database import BasicCollection
            db_name = col.database_name
            # Reconstruct URI
            host, port = new_client.address
            new_col2 = BasicCollection(
                db_name, "col",
                mongo_uri=f"mongodb://{host}:{port}/?directConnection=true",
                operation_mode="preprocess"
            )
            results = list(new_col2.find_many({"city": "PostFail"}))
            assert len(results) >= 1, (
                f"Expected at least 1 result after failover, got {len(results)}"
            )
            assert_chain_integrity(new_col2.collection.collection_versions)
            new_client.close()
        finally:
            primary_container.start()
            time.sleep(15)  # allow rejoining

    def test_operation_after_failover_works(
        self, make_rs_collection, rs_client, docker_client
    ):
        """
        After a failover, registering a new evolution and querying works on new Primary.
        """
        primary_container = _get_primary_container(docker_client)
        primary_container.stop()

        try:
            new_client = _get_rs_client_for_new_primary(timeout=60)
            assert new_client is not None, "No new PRIMARY elected within 60s"

            host, port = new_client.address
            from semantic_heterogeneous_database import BasicCollection
            col = BasicCollection(
                f"mellowtest_failover_after", "col",
                mongo_uri=f"mongodb://{host}:{port}/?directConnection=true",
                operation_mode="preprocess"
            )
            col.insert_one('{"city": "AfterFail", "pop": 5}', datetime(2000, 1, 1))
            col.execute_operation(
                "translation",
                datetime(2010, 1, 1),
                {"fieldName": "city", "oldValue": "AfterFail", "newValue": "AfterFailNew"}
            )
            results = list(col.find_many({"city": "AfterFailNew"}))
            assert len(results) == 1
            assert_chain_integrity(col.collection.collection_versions)
            col.collection.client.drop_database("mellowtest_failover_after")
            new_client.close()
        finally:
            primary_container.start()
            time.sleep(15)

    def test_versions_collection_readable_after_failover(
        self, make_rs_collection, rs_client, docker_client
    ):
        """
        After failover, reading versions from the new primary works.
        """
        primary_container = _get_primary_container(docker_client)
        primary_container.stop()

        try:
            new_client = _get_rs_client_for_new_primary(timeout=60)
            assert new_client is not None, "No new PRIMARY elected within 60s"

            host, port = new_client.address
            from semantic_heterogeneous_database import BasicCollection
            col = BasicCollection(
                "mellowtest_failover_read", "col",
                mongo_uri=f"mongodb://{host}:{port}/?directConnection=true",
                operation_mode="preprocess"
            )
            versions = list(col.collection._versions_r.find())
            assert len(versions) >= 1, "Should find at least the initial version node"
            col.collection.client.drop_database("mellowtest_failover_read")
            new_client.close()
        finally:
            primary_container.start()
            time.sleep(15)
