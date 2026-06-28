"""
Distributed tests for configurable read-node routing.

A MellowDB collection can read the heavy *record* data from a chosen node
(primary or secondary) while keeping the *version chain* on a fresh source:

  - read_mode='split'         -> records from read_uri node, version chain from primary
  - read_mode='single_source' -> records AND version chain from read_uri node

Writes always go to the primary (MongoDB enforces this). Connections are
direct-by-port so the routing works from the host against the Docker replica set.
"""
import time
import uuid
from datetime import datetime
from pymongo import MongoClient
from semantic_heterogeneous_database import BasicCollection


def _discover_member_uris(seed_uri):
    """From any reachable node, return direct-by-port URIs for the primary and a secondary."""
    c = MongoClient(seed_uri, serverSelectionTimeoutMS=3000)
    hello = c.admin.command("hello")
    c.close()
    port = lambda hostport: int(hostport.rsplit(":", 1)[1])
    pri_port = port(hello["primary"])
    sec_port = port(next(h for h in hello["hosts"] if h != hello["primary"]))
    direct = lambda p: f"mongodb://localhost:{p}/?directConnection=true"
    return direct(pri_port), direct(sec_port), pri_port, sec_port


def _eventually(fn, timeout=5.0, interval=0.1):
    """Poll fn() until it returns a truthy value (tolerates replication lag)."""
    deadline = time.time() + timeout
    result = None
    while time.time() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(interval)
    return result


def test_split_mode_routes_records_to_read_node_and_chain_to_primary(rs_uri):
    primary_uri, secondary_uri, pri_port, sec_port = _discover_member_uris(rs_uri)
    db_name = f"mellowtest_rs_{uuid.uuid4().hex[:12]}"
    col = BasicCollection(db_name, "col", primary_uri, "preprocess", "majority",
                          read_uri=secondary_uri, read_mode="split")
    try:
        coll = col.collection
        coll._read_client.admin.command("ping")  # force the read connection to resolve
        # heavy record reads are served by the chosen secondary
        assert coll._processed_read.database.client.address == ("localhost", sec_port)
        # the version chain is still served by the primary
        assert coll._versions_r.database.client.address == ("localhost", pri_port)
    finally:
        col.collection.client.drop_database(db_name)


def test_split_mode_find_many_returns_records_through_secondary(rs_uri):
    primary_uri, secondary_uri, _, _ = _discover_member_uris(rs_uri)
    db_name = f"mellowtest_rs_{uuid.uuid4().hex[:12]}"
    col = BasicCollection(db_name, "col", primary_uri, "preprocess", "majority",
                          read_uri=secondary_uri, read_mode="split")
    try:
        # write goes to the primary; record reads are served by the secondary
        col.insert_one('{"city": "Piracicaba", "pop": 42}', datetime(2001, 1, 1))
        found = _eventually(lambda: list(col.find_many({"city": "Piracicaba"})))
        assert found and len(found) == 1
        assert found[0]["city"] == "Piracicaba"
    finally:
        col.collection.client.drop_database(db_name)


def test_single_source_routes_version_chain_to_read_node(rs_uri):
    primary_uri, secondary_uri, _, sec_port = _discover_member_uris(rs_uri)
    db_name = f"mellowtest_rs_{uuid.uuid4().hex[:12]}"
    col = BasicCollection(db_name, "col", primary_uri, "preprocess", "majority",
                          read_uri=secondary_uri, read_mode="single_source")
    try:
        coll = col.collection
        coll._read_client.admin.command("ping")
        # in single_source, BOTH records and the version chain come from the read node
        assert coll._processed_read.database.client.address == ("localhost", sec_port)
        assert coll._versions_r.database.client.address == ("localhost", sec_port)
    finally:
        col.collection.client.drop_database(db_name)


def test_set_read_source_switches_node_at_runtime(rs_uri):
    primary_uri, secondary_uri, pri_port, sec_port = _discover_member_uris(rs_uri)
    db_name = f"mellowtest_rs_{uuid.uuid4().hex[:12]}"
    # start with the default (records read from the primary connection)
    col = BasicCollection(db_name, "col", primary_uri, "preprocess", "majority")
    try:
        assert col.collection._processed_read.database.client.address == ("localhost", pri_port)
        # switch reads to the secondary live
        col.set_read_source(secondary_uri, "split")
        col.collection._read_client.admin.command("ping")
        assert col.collection._processed_read.database.client.address == ("localhost", sec_port)
        # and back to the primary
        col.set_read_source(None, "split")
        assert col.collection._processed_read.database.client.address == ("localhost", pri_port)
    finally:
        col.collection.client.drop_database(db_name)
