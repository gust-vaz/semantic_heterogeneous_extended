import json
import os

import pytest

from benchmarks.harness.sharding_metrics import as_json, imbalance

needs_cluster = pytest.mark.skipif(
    not os.environ.get("MELLOW_SHARDED"),
    reason="needs a mongos; set MELLOW_SHARDED=1 and point MONGO_HOST at it")


def test_perfect_spread_is_one():
    assert imbalance({"s1": 100, "s2": 100, "s3": 100, "s4": 100}) == 1.0


def test_everything_on_one_shard_is_the_shard_count():
    assert imbalance({"s1": 400, "s2": 0, "s3": 0, "s4": 0}) == 4.0


def test_a_shard_holding_nothing_does_not_make_it_infinite():
    """max/min would divide by zero here, and a zero-holding shard is a normal
    outcome - a ranged key on a small corpus produces one every time."""
    assert imbalance({"s1": 0, "s2": 6, "s3": 10, "s4": 2}) == pytest.approx(10 / 4.5)


def test_an_empty_distribution_is_balanced_by_definition():
    assert imbalance({}) == 0.0
    assert imbalance({"s1": 0, "s2": 0}) == 0.0


def test_as_json_round_trips():
    assert json.loads(as_json({"s1": 1, "s2": 2})) == {"s1": 1, "s2": 2}


def test_as_json_is_stable_so_rows_can_be_diffed():
    assert as_json({"s2": 2, "s1": 1}) == as_json({"s1": 1, "s2": 2})


@needs_cluster
def test_owned_documents_exclude_orphans(primary_uri):
    """After a migration a shard keeps the documents it handed over until the
    range deleter runs. Counting through a direct connection to each shard would
    include them: one measured run had 7613 owned and 12387 orphaned on the same
    shard. distribution() must report only what the shard actually owns."""
    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import distribution, orphans

    client = MongoClient(primary_uri)
    client.drop_database("orphan_probe")
    client.admin.command("enableSharding", "orphan_probe")
    database = client["orphan_probe"]
    database.create_collection("c")
    client.admin.command("shardCollection", "orphan_probe.c", key={"k": 1})
    database["c"].insert_many([{"k": i, "pad": "x" * 400} for i in range(20000)])
    for point in (5000, 10000, 15000):
        client.admin.command("split", "orphan_probe.c", middle={"k": point})

    shards = sorted(entry["_id"] for entry in client["config"].shards.find())
    if len(shards) > 1:
        client.admin.command("moveChunk", "orphan_probe.c",
                             find={"k": 7500}, to=shards[1])

    try:
        owned = distribution(client, "orphan_probe.c")
        assert sum(owned.values()) == database["c"].count_documents({})
        assert set(orphans(client, "orphan_probe.c")) == set(owned)
    finally:
        client.drop_database("orphan_probe")
