import os

import pytest

from benchmarks.experiments.s5_schemaless import (
    KEY_COLUMNS, MISSING_FRACTIONS, SHARD_KEY, strip_shard_key,
)

needs_cluster = pytest.mark.skipif(
    not os.environ.get("MELLOW_SHARDED"),
    reason="needs a mongos; set MELLOW_SHARDED=1 and point MONGO_HOST at it")


def test_the_fractions_span_none_to_all():
    assert MISSING_FRACTIONS == [0.0, 0.25, 0.5, 1.0]


def test_the_shard_key_is_a_field_the_operation_never_touches():
    """The driver path only exists where a document can legitimately lack the
    shard key, and that is the middle row of the matrix: never _id, which every
    document has, and never the evolving field, which the operation needs."""
    assert SHARD_KEY == {"role": "data", "kind": "hashed"}


def test_the_key_columns_carry_the_missing_fraction():
    assert "missing_fraction" in KEY_COLUMNS
    for column in ["experiment", "deployment", "corpus", "operation_mode",
                   "repetition"]:
        assert column in KEY_COLUMNS


def test_stripping_removes_the_requested_share(primary_uri):
    from pymongo import MongoClient

    client = MongoClient(primary_uri)
    client.drop_database("strip_probe")
    collection = client["strip_probe"]["c"]
    collection.insert_many([{"k": index, "f0": "SP"} for index in range(100)])
    try:
        assert strip_shard_key(collection, "f0", 0.25) == 25
        assert collection.count_documents({"f0": None}) == 25
        assert collection.count_documents({"f0": "SP"}) == 75
    finally:
        client.drop_database("strip_probe")


def test_stripping_nothing_changes_nothing(primary_uri):
    from pymongo import MongoClient

    client = MongoClient(primary_uri)
    client.drop_database("strip_probe")
    collection = client["strip_probe"]["c"]
    collection.insert_many([{"k": index, "f0": "SP"} for index in range(10)])
    try:
        assert strip_shard_key(collection, "f0", 0.0) == 0
        assert collection.count_documents({"f0": None}) == 0
    finally:
        client.drop_database("strip_probe")


def test_stripping_everything_leaves_no_document_with_the_key(primary_uri):
    from pymongo import MongoClient

    client = MongoClient(primary_uri)
    client.drop_database("strip_probe")
    collection = client["strip_probe"]["c"]
    collection.insert_many([{"k": index, "f0": "SP"} for index in range(40)])
    try:
        assert strip_shard_key(collection, "f0", 1.0) == 40
        assert collection.count_documents({"f0": {"$exists": True}}) == 0
    finally:
        client.drop_database("strip_probe")


def test_stripping_is_deterministic_so_repetitions_compare(primary_uri):
    """Chosen by sorted _id rather than at random: the same fraction must remove
    the same documents every time, or the repetitions differ in two ways."""
    from pymongo import MongoClient

    client = MongoClient(primary_uri)
    removed = []
    for _ in range(2):
        client.drop_database("strip_probe")
        collection = client["strip_probe"]["c"]
        collection.insert_many([{"k": index, "f0": "SP"} for index in range(40)])
        strip_shard_key(collection, "f0", 0.5)
        removed.append(sorted(document["k"] for document
                              in collection.find({"f0": None}, {"k": 1})))
    client.drop_database("strip_probe")
    assert removed[0] == removed[1]


def test_stripping_preserves_every_other_field_and_the_id(primary_uri):
    """Done by delete and re-insert, so nothing else about the document may
    change - MongoDB refuses a bulk $unset of a shard key value, and refuses a
    single-document one that does not name the full key."""
    from pymongo import MongoClient

    client = MongoClient(primary_uri)
    client.drop_database("strip_probe")
    collection = client["strip_probe"]["c"]
    collection.insert_many(
        [{"k": index, "f0": "SP", "keep": f"x{index}"} for index in range(20)])
    before = {document["k"]: (document["_id"], document["keep"])
              for document in collection.find()}
    try:
        strip_shard_key(collection, "f0", 0.5)
        after = {document["k"]: (document["_id"], document["keep"])
                 for document in collection.find()}
        assert after == before
        assert collection.count_documents({}) == 20
    finally:
        client.drop_database("strip_probe")


@needs_cluster
def test_stripping_works_on_a_sharded_collection(primary_uri):
    """The case a bulk $unset cannot do: removing a shard key value changes
    where the document routes, and MongoDB refuses that as a multi-update."""
    from pymongo import MongoClient

    client = MongoClient(primary_uri)
    client.drop_database("strip_sharded")
    client.admin.command("enableSharding", "strip_sharded")
    client["strip_sharded"].create_collection("c")
    client.admin.command("shardCollection", "strip_sharded.c",
                         key={"f0": "hashed"})
    collection = client["strip_sharded"]["c"]
    collection.insert_many([{"k": i, "f0": f"v{i % 20}"} for i in range(500)])
    try:
        assert strip_shard_key(collection, "f0", 0.5) == 250
        assert collection.count_documents({"f0": None}) == 250
        assert collection.count_documents({}) == 500
    finally:
        client.drop_database("strip_sharded")
