import os

import pytest

from benchmarks.harness.cell_setup import (
    SHARD_KEY_MATRIX, assert_distribution, expects_spread, key_pattern,
)

needs_cluster = pytest.mark.skipif(
    not os.environ.get("MELLOW_SHARDED"),
    reason="needs a mongos; set MELLOW_SHARDED=1 and point MONGO_HOST at it")


def test_the_matrix_is_three_roles_by_two_kinds():
    assert len(SHARD_KEY_MATRIX) == 6
    assert {cell["role"] for cell in SHARD_KEY_MATRIX} == {"id", "data", "evolved"}
    assert {cell["kind"] for cell in SHARD_KEY_MATRIX} == {"hashed", "ranged"}
    assert len({(c["role"], c["kind"]) for c in SHARD_KEY_MATRIX}) == 6


def test_the_ranged_id_cell_is_declared_a_negative_control():
    cell = next(c for c in SHARD_KEY_MATRIX
                if c["role"] == "id" and c["kind"] == "ranged")
    assert cell["negative_control"] is True


def test_no_other_cell_is_a_negative_control():
    assert sum(1 for c in SHARD_KEY_MATRIX if c["negative_control"]) == 1


def test_key_pattern_speaks_mongodbs_own_vocabulary():
    assert key_pattern("municipio", "hashed") == {"municipio": "hashed"}
    assert key_pattern("municipio", "ranged") == {"municipio": 1}


def test_only_the_ranged_id_control_is_allowed_to_concentrate():
    control = next(c for c in SHARD_KEY_MATRIX if c["negative_control"])
    other = next(c for c in SHARD_KEY_MATRIX if not c["negative_control"])
    assert expects_spread(4, control) is False
    assert expects_spread(4, other) is True


def test_a_one_shard_deployment_never_expects_a_spread():
    assert expects_spread(1, SHARD_KEY_MATRIX[0]) is False


def test_the_guard_passes_a_spread_distribution():
    assert_distribution({"s1": 10, "s2": 10}, expect_spread=True, label="cell")


def test_the_guard_fails_a_concentrated_distribution():
    """Two spike rounds measured nothing because every document sat on one
    shard, and every number looked plausible. A cell that expected to spread
    must refuse to produce a number."""
    with pytest.raises(RuntimeError) as exc:
        assert_distribution({"s1": 20, "s2": 0}, expect_spread=True, label="sh4/evo0")
    assert "sh4/evo0" in str(exc.value)


def test_the_guard_allows_concentration_when_the_cell_expects_it():
    assert_distribution({"s1": 20, "s2": 0}, expect_spread=False, label="control")


def test_the_guard_fails_an_empty_distribution_either_way():
    for expect_spread in (True, False):
        with pytest.raises(RuntimeError):
            assert_distribution({}, expect_spread=expect_spread, label="empty")
        with pytest.raises(RuntimeError):
            assert_distribution({"s1": 0}, expect_spread=expect_spread, label="zero")


@needs_cluster
def test_presplitting_spreads_a_ranged_collection(primary_uri):
    """Measured in Phase 2: a 467-document ranged collection sat entirely on one
    shard, because a ranged key starts as a single chunk and MongoDB only splits
    at the chunk size. Pre-splitting is what makes the three ranged cells
    measurable at all."""
    from pymongo import MongoClient

    from benchmarks.harness.cell_setup import presplit
    from benchmarks.harness.sharding_metrics import distribution, set_balancer

    client = MongoClient(primary_uri)
    shards = sorted(entry["_id"] for entry in client["config"].shards.find())
    if len(shards) < 2:
        pytest.skip("needs more than one shard")
    set_balancer(client, False)
    client.drop_database("presplit_probe")
    client.admin.command("enableSharding", "presplit_probe")
    client["presplit_probe"].create_collection("c")
    client.admin.command("shardCollection", "presplit_probe.c", key={"k": 1})
    client["presplit_probe"]["c"].insert_many([{"k": i} for i in range(400)])

    try:
        before = distribution(client, "presplit_probe.c")
        assert len([count for count in before.values() if count]) == 1

        created = presplit(client, "presplit_probe.c", "k", [100, 200, 300], shards)
        assert created == 3

        after = distribution(client, "presplit_probe.c")
        assert len([count for count in after.values() if count]) > 1
        assert sum(after.values()) == 400
        assert_distribution(after, expect_spread=True, label="presplit_probe")
    finally:
        client.drop_database("presplit_probe")


@needs_cluster
def test_presplit_placement_is_deterministic(primary_uri):
    """Two runs of the same cell must start from the same layout, or the
    campaign is comparing cells that were not set up alike. Placement is
    round-robin over the shard list rather than whatever the balancer picks.
    """
    from pymongo import MongoClient

    from benchmarks.harness.cell_setup import presplit
    from benchmarks.harness.sharding_metrics import chunk_counts, set_balancer

    client = MongoClient(primary_uri)
    shards = sorted(entry["_id"] for entry in client["config"].shards.find())
    if len(shards) < 2:
        pytest.skip("needs more than one shard")
    set_balancer(client, False)

    def build(name):
        client.drop_database(name)
        client.admin.command("enableSharding", name)
        client[name].create_collection("c")
        client.admin.command("shardCollection", f"{name}.c", key={"k": 1})
        client[name]["c"].insert_many([{"k": i} for i in range(400)])
        presplit(client, f"{name}.c", "k", [100, 200, 300], shards)
        return chunk_counts(client, f"{name}.c")

    try:
        assert build("presplit_a") == build("presplit_b")
    finally:
        client.drop_database("presplit_a")
        client.drop_database("presplit_b")
