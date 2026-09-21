"""Instruments for measuring a sharded cluster.

Distribution comes from $shardedDataDistribution and nothing else. Counting
through a direct connection to each shard also counts orphans - documents a
migration handed over but whose copies the source shard has not deleted yet -
and right after a rebalance one shard was measured holding 7613 live documents
and 12387 orphans. That number would have been wrong by 160% with nothing on the
surface to suggest it.
"""

import json


def _entries(client, namespace):
    """The per-shard entries $shardedDataDistribution reports for one namespace.

    Empty for an unsharded collection, and for one that does not exist: the
    aggregation simply yields no document rather than raising.
    """
    result = client["admin"].aggregate([
        {"$shardedDataDistribution": {}},
        {"$match": {"ns": namespace}},
    ])
    for document in result:
        return document["shards"]
    return []


def distribution(client, namespace):
    """Live documents per shard. Orphans are excluded."""
    return {entry["shardName"]: entry["numOwnedDocuments"]
            for entry in _entries(client, namespace)}


def orphans(client, namespace):
    """Documents per shard that a migration left behind, awaiting cleanup.

    Not part of any logical count, but real bytes on real disks for as long as
    the range deleter takes - so the cost of rebalancing reports them separately
    rather than hiding them.
    """
    return {entry["shardName"]: entry["numOrphanedDocs"]
            for entry in _entries(client, namespace)}


def imbalance(dist):
    """How far the heaviest shard sits above a fair share. 1.0 is perfect.

    max/mean rather than max/min: a shard holding nothing is an ordinary result -
    a ranged key on a small corpus produces one every time - and max/min would
    report infinity for exactly the cells worth looking at.
    """
    counts = list(dist.values())
    total = sum(counts)
    if not counts or total == 0:
        return 0.0
    return max(counts) / (total / len(counts))


def as_json(dist):
    """The distribution as one CSV cell, so any index can be recomputed later.

    Sorted, so two rows with the same distribution compare equal as text.
    """
    return json.dumps(dist, sort_keys=True)
