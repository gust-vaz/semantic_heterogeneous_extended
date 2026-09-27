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


def matched_by_shard(client, namespace, filter_):
    """How many documents matching `filter_` live on each shard.

    mongos answers a count with one total, and $shardedDataDistribution takes no
    filter, so the per-shard breakdown comes from the query planner's execution
    stats. Shards the query was sent to appear even when they matched nothing,
    which is what makes the result usable as evidence of targeting.
    """
    database, _, collection = namespace.partition(".")
    plan = client[database].command(
        "explain", {"find": collection, "filter": filter_},
        verbosity="executionStats")
    stages = plan["executionStats"]["executionStages"]
    shards = stages.get("shards")
    if shards is None:
        # not a cluster, or an unsharded collection: one implicit shard
        return {"": stages.get("nReturned", 0)}
    return {entry["shardName"]: entry.get("nReturned", 0) for entry in shards}


def relocated(before, after, originals):
    """Copies that landed on a shard other than the one their original sits on.

    `originals` is the per-shard distribution of the documents the operation
    touched, from matched_by_shard. A single origin shard would not do: that
    only exists when the shard key is the field being rewritten. When the key is
    a field the operation never touches, every copy keeps its key value and
    routes back beside its own original - across as many shards as those
    originals occupy - and the answer must be zero.

    Only valid with the balancer off. With it on, chunk migrations move documents
    too, and the two movements add up into one number that cannot be decomposed
    afterwards - which is why the balancer being off is a requirement of the
    measurement, not a preference.

    Counted per shard and clamped at zero: a shard that received fewer copies
    than it had originals must not cancel out a genuine arrival elsewhere.
    """
    total = 0
    for shard in set(before) | set(after) | set(originals):
        arrivals = after.get(shard, 0) - before.get(shard, 0)
        total += max(arrivals - originals.get(shard, 0), 0)
    return total
