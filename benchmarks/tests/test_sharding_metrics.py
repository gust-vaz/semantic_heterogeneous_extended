import json
import os

import pytest

from benchmarks.harness.sharding_metrics import as_json, imbalance, relocated

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


def test_copies_landing_where_their_originals_live_did_not_relocate():
    before = {"s1": 100, "s2": 100}
    after = {"s1": 120, "s2": 100}
    assert relocated(before, after, originals={"s1": 20}) == 0


def test_copies_landing_elsewhere_all_relocated():
    before = {"s1": 100, "s2": 100}
    after = {"s1": 100, "s2": 120}
    assert relocated(before, after, originals={"s1": 20}) == 20


def test_originals_spread_across_shards_relocate_nothing():
    """The case a single origin shard cannot express. With the shard key on a
    field the operation never touches, every copy keeps its key value and routes
    back to the shard its original sits on - however many shards that is."""
    before = {"s1": 20, "s2": 10, "s3": 20, "s4": 10}
    after = {"s1": 40, "s2": 20, "s3": 40, "s4": 20}
    assert relocated(before, after, originals={"s1": 20, "s2": 10,
                                               "s3": 20, "s4": 10}) == 0


def test_only_arrivals_beyond_the_originals_count():
    # half the copies stayed, half went elsewhere
    before = {"s1": 20, "s2": 0}
    after = {"s1": 30, "s2": 10}
    assert relocated(before, after, originals={"s1": 20}) == 10


def test_a_shard_that_appears_only_after_counts_as_arrivals():
    before = {"s1": 100}
    after = {"s1": 100, "s2": 20}
    assert relocated(before, after, originals={"s1": 20}) == 20


def test_shrinking_shards_never_count_as_negative_arrivals():
    """With the balancer off this should not happen. If it ever does, a negative
    arrival count would silently cancel out a real relocation somewhere else."""
    before = {"s1": 100, "s2": 100}
    after = {"s1": 90, "s2": 130}
    assert relocated(before, after, originals={"s1": 20}) == 30


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


@needs_cluster
@pytest.mark.parametrize("shard_field, expect_relocation", [
    ("bairro", False),   # the operation does not touch the shard key
    ("cidade", True),    # the shard key is the field that evolves
])
def test_relocation_happens_only_when_the_shard_key_evolves(
        primary_uri, shard_field, expect_relocation):
    """The pair that pins the headline measurement from both sides. An evolved
    copy keeps every field but the one the operation rewrites, so it routes to
    the same shard unless the shard key itself changed value."""
    from datetime import datetime

    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import distribution, matched_by_shard
    from semantic_heterogeneous_database import BasicCollection

    client = MongoClient(primary_uri)
    client.drop_database("reloc_probe")
    collection = BasicCollection(
        "reloc_probe", "c", primary_uri, "preprocess",
        shard_key={shard_field: "hashed"})
    for index in range(60):
        collection.insert_one(
            '{"cidade": "SP", "bairro": "b%d"}' % (index % 6),
            datetime(2020, 1, 1))

    namespace = "reloc_probe.c_processed"
    try:
        before = distribution(client, namespace)
        originals = matched_by_shard(client, namespace, {"cidade": "SP"})
        collection.execute_operation(
            "translation", datetime(2021, 1, 1),
            {"fieldName": "cidade", "oldValue": "SP", "newValue": "SAO PAULO"})
        after = distribution(client, namespace)

        assert sum(after.values()) == sum(before.values()) + 60
        moved = relocated(before, after, originals=originals)
        if expect_relocation:
            assert moved > 0
        else:
            assert moved == 0
    finally:
        client.drop_database("reloc_probe")


@needs_cluster
def test_matched_by_shard_counts_the_documents_a_filter_finds_on_each_shard():
    """The per-shard breakdown a plain count cannot give: mongos returns one
    total, and $shardedDataDistribution takes no filter."""
    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import matched_by_shard

    import os as _os
    client = MongoClient(_os.environ.get("MONGO_HOST", "mongodb://localhost:27017"))
    client.drop_database("matched_probe")
    client.admin.command("enableSharding", "matched_probe")
    database = client["matched_probe"]
    database.create_collection("c")
    client.admin.command("shardCollection", "matched_probe.c", key={"k": "hashed"})
    database["c"].insert_many(
        [{"k": f"k{i % 6}", "tag": "yes"} for i in range(60)]
        + [{"k": f"k{i % 6}", "tag": "no"} for i in range(30)])
    try:
        counts = matched_by_shard(client, "matched_probe.c", {"tag": "yes"})
        assert sum(counts.values()) == 60
        assert sum(counts.values()) == database["c"].count_documents({"tag": "yes"})
        if client["config"].shards.count_documents({}) > 1:
            assert len([n for n in counts.values() if n]) > 1
    finally:
        client.drop_database("matched_probe")
