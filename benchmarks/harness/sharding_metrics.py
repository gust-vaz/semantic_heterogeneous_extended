"""Instruments for measuring a sharded cluster.

Distribution comes from $shardedDataDistribution and nothing else. Counting
through a direct connection to each shard also counts orphans - documents a
migration handed over but whose copies the source shard has not deleted yet -
and right after a rebalance one shard was measured holding 7613 live documents
and 12387 orphans. That number would have been wrong by 160% with nothing on the
surface to suggest it.
"""

import json
import time
import time


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


def set_chunk_size(client, megabytes):
    """Set the cluster-wide chunk size, in MB.

    MongoDB stores no chunksize setting by default - the 128 MB is built in -
    and the balancer moves data only when a collection is unbalanced by size.
    At any corpus size this campaign can afford, 128 MB makes the balancer
    inert: a measured run of 4 chunks holding 956 KB produced zero migrations
    and looked exactly like a balancer that does not work.
    """
    client["config"].settings.update_one(
        {"_id": "chunksize"}, {"$set": {"value": megabytes}}, upsert=True)


def chunk_counts(client, namespace):
    """Chunks per shard for this collection. Empty if it is not sharded."""
    config = client["config"]
    entry = config.collections.find_one(
        {"_id": namespace, "dropped": {"$ne": True}})
    if entry is None:
        return {}
    counts = {}
    for chunk in config.chunks.find({"uuid": entry["uuid"]}, {"shard": 1}):
        counts[chunk["shard"]] = counts.get(chunk["shard"], 0) + 1
    return counts


def set_balancer(client, running):
    """Start or stop the balancer cluster-wide."""
    client.admin.command("balancerStart" if running else "balancerStop")


#: One migration writes several changelog entries (moveChunk.start,
#: moveChunk.from, moveChunk.to and more). Counting and slicing must use the
#: same filter, or the slice length is taken from a different population than
#: the rows it slices.
MOVE_FILTER = {"what": "moveChunk.from"}


def move_marker(client):
    """How many completed migrations the changelog holds right now."""
    return client["config"].changelog.count_documents(MOVE_FILTER)


def moves_since(client, marker):
    """The migrations recorded after `marker`, newest first.

    Each carries a six-step timing breakdown plus min, max, to, from and note,
    so the cost of rebalancing decomposes per migration instead of arriving as
    one opaque total.
    """
    new_count = max(move_marker(client) - marker, 0)
    if new_count == 0:
        return []
    return list(client["config"].changelog.find(MOVE_FILTER)
                .sort("time", -1).limit(new_count))


def wait_for_balancer(client, namespace, timeout_s=300, poll_s=2.0):
    """Seconds until the balancer settles, or None if it did not in time.

    Three conditions, listed with what the evidence actually shows each one
    does. Measured on a four-shard cluster, 20k documents in four pre-split
    chunks at chunkSize 1 MB:

    1. Not currently inside a balancer round. Does most of the work:
       inBalancerRound stays true for the whole migration burst (t=8s to t=16s
       in the measured run) and drops when it is over.
    2. At least three rounds have elapsed since the wait began. Required.
       Without it the wait returns on its first poll, about 8 s before the
       balancer starts its first round, with nothing moved yet - which is what
       the first version of this function did, reporting 0.0s.
    3. The chunk layout is unchanged since the previous poll. NOT exercised by
       the test suite: on a collection this small the balancer never goes idle
       with work still to do, so removing this condition changes only the
       moment of return, not the answer. Kept as a guard for the campaign's
       far larger collections, where a pause between migration bursts is
       plausible and conditions 1 and 2 would both be satisfied mid-rebalance.
    """
    started = time.time()
    rounds_at_start = client.admin.command("balancerStatus")["numBalancerRounds"]
    previous = None
    while time.time() - started < timeout_s:
        time.sleep(poll_s)
        status = client.admin.command("balancerStatus")
        layout = chunk_counts(client, namespace)
        settled = (not status["inBalancerRound"]
                   and status["numBalancerRounds"] >= rounds_at_start + 3
                   and previous is not None and layout == previous)
        previous = layout
        if settled:
            return round(time.time() - started, 3)
    return None
