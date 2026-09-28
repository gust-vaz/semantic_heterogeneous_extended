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


@needs_cluster
def test_chunk_counts_sum_to_the_collections_chunks(primary_uri):
    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import chunk_counts, set_chunk_size

    client = MongoClient(primary_uri)
    set_chunk_size(client, 1)
    client.drop_database("chunk_probe")
    client.admin.command("enableSharding", "chunk_probe")
    client["chunk_probe"].create_collection("c")
    client.admin.command("shardCollection", "chunk_probe.c", key={"k": 1})
    for point in (10, 20, 30):
        client.admin.command("split", "chunk_probe.c", middle={"k": point})
    try:
        assert sum(chunk_counts(client, "chunk_probe.c").values()) == 4
    finally:
        client.drop_database("chunk_probe")


@needs_cluster
def test_chunk_counts_is_empty_for_a_collection_that_is_not_sharded(primary_uri):
    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import chunk_counts

    client = MongoClient(primary_uri)
    client.drop_database("unsharded_probe")
    client["unsharded_probe"]["c"].insert_one({"k": 1})
    try:
        assert chunk_counts(client, "unsharded_probe.c") == {}
    finally:
        client.drop_database("unsharded_probe")


@needs_cluster
def test_the_balancer_converges_and_actually_moves_chunks(primary_uri):
    import time

    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import (
        chunk_counts, move_marker, moves_since, set_balancer, set_chunk_size,
        wait_for_balancer)

    client = MongoClient(primary_uri)
    if client["config"].shards.count_documents({}) < 2:
        pytest.skip("needs more than one shard to rebalance across")
    set_balancer(client, False)
    # MongoDB stores no chunksize setting by default and the built-in 128 MB
    # makes the balancer inert at any corpus size this campaign can afford.
    set_chunk_size(client, 1)
    client.drop_database("balance_probe")
    client.admin.command("enableSharding", "balance_probe")
    client["balance_probe"].create_collection("c")
    client.admin.command("shardCollection", "balance_probe.c", key={"k": 1})
    client["balance_probe"]["c"].insert_many(
        [{"k": i, "pad": "x" * 400} for i in range(20000)])
    for point in (5000, 10000, 15000):
        client.admin.command("split", "balance_probe.c", middle={"k": point})

    try:
        assert len(chunk_counts(client, "balance_probe.c")) == 1
        marker = move_marker(client)
        set_balancer(client, True)
        elapsed = wait_for_balancer(client, "balance_probe.c", timeout_s=300)
        assert elapsed is not None, "balancer did not converge"
        assert len(chunk_counts(client, "balance_probe.c")) > 1
        moves = moves_since(client, marker)
        assert moves, "chunks moved but the changelog reported none"
        assert all(entry["what"] == "moveChunk.from" for entry in moves)

        # Converged means finished, not started. Measured on this cluster: the
        # balancer moves about one chunk per round, so a four-chunk collection
        # keeps changing for a dozen rounds. Returning as soon as movement
        # began would pass every assertion above and still be wrong, so the
        # layout has to hold still afterwards.
        settled = chunk_counts(client, "balance_probe.c")
        time.sleep(12)
        assert chunk_counts(client, "balance_probe.c") == settled, (
            "the balancer was still moving chunks when convergence was declared")
    finally:
        set_balancer(client, False)
        client.drop_database("balance_probe")


@needs_cluster
def test_convergence_is_not_declared_before_anything_happens(primary_uri):
    """The first version of this detector compared numBalancerRounds against a
    counter starting at zero, so it passed on its first poll and reported 0.0s.
    A namespace the balancer will never touch must not converge instantly."""
    import time

    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import set_balancer, wait_for_balancer

    client = MongoClient(primary_uri)
    set_balancer(client, True)
    try:
        started = time.time()
        wait_for_balancer(client, "nonexistent.collection", timeout_s=20)
        assert time.time() - started >= 5
    finally:
        set_balancer(client, False)


@needs_cluster
def test_a_collection_on_one_shard_reports_the_others_as_empty(primary_uri):
    """$shardedDataDistribution omits shards holding nothing, and an imbalance
    computed over the occupied ones alone calls a collection sitting entirely on
    one shard of four perfectly balanced. A ranged _id concentrates by
    construction, which is why it is kept as the matrix's negative control."""
    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import distribution, imbalance

    client = MongoClient(primary_uri)
    shard_count = client["config"].shards.count_documents({})
    if shard_count < 2:
        pytest.skip("needs more than one shard")
    client.drop_database("imbalance_probe")
    client.admin.command("enableSharding", "imbalance_probe")
    client["imbalance_probe"].create_collection("c")
    client.admin.command("shardCollection", "imbalance_probe.c", key={"_id": 1})
    client["imbalance_probe"]["c"].insert_many([{"n": i} for i in range(400)])

    try:
        dist = distribution(client, "imbalance_probe.c")
        assert len(dist) == shard_count
        assert sum(1 for count in dist.values() if count) == 1
        assert imbalance(dist) == pytest.approx(shard_count)
    finally:
        client.drop_database("imbalance_probe")


@needs_cluster
@pytest.mark.parametrize("kind", ["hashed", "ranged"])
def test_a_dominant_shard_key_value_produces_a_chunk_nothing_can_move(
        primary_uri, kind):
    """A chunk boundary can only fall between distinct values, so one value
    holding more than the chunk size gives a chunk MongoDB can neither split
    nor migrate. A skewed semantic shard key produces exactly that, which is a
    result of the campaign rather than an obstacle to it."""
    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import (
        oversized_chunks, set_balancer, set_chunk_size)

    client = MongoClient(primary_uri)
    set_balancer(client, False)
    set_chunk_size(client, 1)
    client.drop_database("jumbo_probe")
    client.admin.command("enableSharding", "jumbo_probe")
    client["jumbo_probe"].create_collection("c")
    pattern = {"k": "hashed" if kind == "hashed" else 1}
    client.admin.command("shardCollection", "jumbo_probe.c", key=pattern)
    client["jumbo_probe"]["c"].insert_many(
        [{"k": "hot", "pad": "x" * 350} for _ in range(18000)]
        + [{"k": f"v{i % 50:03d}", "pad": "x" * 350} for i in range(2000)])

    try:
        assert oversized_chunks(client, "jumbo_probe.c", pattern, 1) >= 1
        # the same collection under a chunk size large enough to hold it has none
        assert oversized_chunks(client, "jumbo_probe.c", pattern, 64) == 0
    finally:
        client.drop_database("jumbo_probe")


@needs_cluster
def test_a_collection_that_is_not_sharded_has_no_oversized_chunks(primary_uri):
    from pymongo import MongoClient

    from benchmarks.harness.sharding_metrics import oversized_chunks

    client = MongoClient(primary_uri)
    client.drop_database("nochunk_probe")
    client["nochunk_probe"]["c"].insert_one({"k": 1})
    try:
        assert oversized_chunks(client, "nochunk_probe.c", {"k": 1}, 1) == 0
    finally:
        client.drop_database("nochunk_probe")
