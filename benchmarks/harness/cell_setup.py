"""Putting one cell's cluster into the state it declares, and refusing to
measure when it is not in that state.

The guard exists because two rounds of the Phase 2 spike measured nothing: every
document sat on one shard, and every number looked plausible. A sharding
measurement that does not check its own distribution first proves nothing.
"""

import time

from pymongo.errors import OperationFailure

from benchmarks.harness import sharding_metrics

#: moveChunk refusing a chunk bigger than the chunk size. Expected whenever a
#: single shard key value outgrows a chunk, which a skewed semantic key does by
#: construction - the campaign counts those chunks rather than failing on them.
CHUNK_TOO_BIG = 153

#: moveChunk refused because a shard involved in it is still finishing a previous
#: migration (its range deletion outlives the moveChunk that triggered it). On a
#: real multi-VM cluster that cleanup takes seconds; on single-host Docker it is
#: instant, so this only surfaced in the cloud. The move succeeds once it clears.
CONFLICTING_OPERATION_IN_PROGRESS = 117


def move_chunk(client, namespace, chunk, to, attempts=12, delay_s=5):
    """Move one chunk, waiting out a migration still settling on an involved shard.

    `_waitForDelete` makes each move wait for its own range deletion, and a retry
    on ConflictingOperationInProgress rides out a previous move's cleanup. A chunk
    bigger than the chunk size cannot move and is swallowed (counted elsewhere);
    any other failure is real and raised.
    """
    for attempt in range(attempts):
        try:
            client.admin.command("moveChunk", namespace,
                                 bounds=[chunk["min"], chunk["max"]],
                                 to=to, _waitForDelete=True)
            return
        except OperationFailure as failure:
            if failure.code == CHUNK_TOO_BIG:
                return
            if (failure.code == CONFLICTING_OPERATION_IN_PROGRESS
                    and attempt < attempts - 1):
                time.sleep(delay_s)
                continue
            raise

#: The shard key matrix. The axis that matters is `role` - how the key relates
#: to semantic evolution - not `kind`. `_id` never carries a data value, and
#: split_processed_records unsets it before the $merge, so a copy is born with a
#: fresh id and lands at random: that row is the control the other two are
#: measured against.
SHARD_KEY_MATRIX = [
    {"role": "id", "kind": "hashed", "negative_control": False},
    {"role": "id", "kind": "ranged", "negative_control": True},
    {"role": "data", "kind": "hashed", "negative_control": False},
    {"role": "data", "kind": "ranged", "negative_control": False},
    {"role": "evolved", "kind": "hashed", "negative_control": False},
    {"role": "evolved", "kind": "ranged", "negative_control": False},
]


def key_pattern(field, kind):
    """The dict shardCollection takes, from a matrix cell."""
    return {field: "hashed" if kind == "hashed" else 1}


def expects_spread(shard_count, cell):
    """Whether this cell's corpus should end up spread across shards.

    One cell is expected to concentrate: ranged `_id`. ObjectId is monotonically
    increasing, so every insert lands in the last chunk - which is the whole
    point of keeping it as a declared negative control. Everything else, on a
    deployment with more than one shard, must spread or it measures nothing.
    """
    return shard_count > 1 and not cell["negative_control"]


def presplit(client, namespace, field, points, shards):
    """Split a ranged collection at `points` and spread the chunks round-robin.

    A ranged collection is born as one chunk and MongoDB only splits at the
    chunk size, so without this a small corpus sits entirely on one shard and
    the cell measures nothing. Splitting alone is not enough with the balancer
    off: the chunks have to be moved too.

    Placement is round-robin over `shards` rather than whatever the balancer
    would pick, so two runs of the same cell start from the same layout.

    EVERY chunk is moved, not just the ones the split points name. The first
    chunk - MinKey up to the first point - is otherwise left wherever the
    database's primary shard happens to be, and MongoDB rotates that between
    databases, so two runs of the same cell would start from different layouts.
    Chunks are addressed by bounds rather than by a contained value, because
    MinKey is not a value the caller can name.

    Not idempotent, and does not need to be: every cell builds its own corpus
    under a fresh database name. Measured on 8.0.12, splitting at a point that
    is already a chunk boundary fails with "new split key ... is a boundary key
    of existing chunk", while moving a chunk to the shard that already owns it
    is accepted as a no-op - which is why the destination is not checked first.
    """
    for point in points:
        client.admin.command("split", namespace, middle={field: point})

    config = client["config"]
    entry = config.collections.find_one(
        {"_id": namespace, "dropped": {"$ne": True}})
    chunks = list(config.chunks.find({"uuid": entry["uuid"]})
                  .sort(f"min.{field}", 1))
    for index, chunk in enumerate(chunks):
        ## move_chunk swallows an oversized chunk (counted elsewhere) and waits
        ## out a migration still settling on an involved shard; any other failure
        ## is real and stops the cell.
        move_chunk(client, namespace, chunk, shards[index % len(shards)])
    return len(points)


def assert_distribution(dist, expect_spread, label):
    """Refuse to measure a cell whose data is not where the cell says it is."""
    occupied = [count for count in dist.values() if count > 0]
    if not occupied:
        raise RuntimeError(
            f"cell {label}: the collection is empty on every shard; there is "
            f"nothing to measure")
    if expect_spread and len(occupied) < 2:
        raise RuntimeError(
            f"cell {label}: expected the corpus to be spread across shards but "
            f"all {sum(occupied)} documents sit on one. Pre-split the chunks or "
            f"raise the corpus size - measuring this cell would prove nothing.")


def prepare(client, chunk_size_mb, balancer):
    """Cluster-wide state a cell declares before it builds its corpus."""
    sharding_metrics.set_chunk_size(client, chunk_size_mb)
    sharding_metrics.set_balancer(client, balancer)
