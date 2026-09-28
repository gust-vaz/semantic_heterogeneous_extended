"""S2 - what it costs to put the cluster back in balance afterwards.

Rides along on S1's core cells rather than building its own corpus: setup is
99% of a cell's cost, and S2 measures a second thing about the state S1 has
just created. The pair is the full answer to what a semantic shard key costs -
applying the operation, then restoring balance - and both halves are zero when
the key is `_id`.

There is no main(): this experiment has no sweep of its own. `shard-bench.sh s2`
is accepted so the name resolves, but the rows are written by S1's run.
"""

import time

from benchmarks.harness import sharding_metrics, shard_runner

EXPERIMENT = "s2"
BALANCER = True
RIDES_ALONG_WITH_S1 = True

KEY_COLUMNS = ["experiment", "deployment", "corpus", "operation_mode",
               "shard_key_role", "shard_key_kind", "skew",
               "shard_key_cardinality", "repetition"]


def rebalance_row(client, args, s1_row, namespace, mongo_version):
    """Turn the balancer on over S1's end state and measure the recovery.

    Requires the reduced chunk size S1 already set: at MongoDB's built-in
    128 MB the balancer never runs on a corpus this size, and every column here
    would read as a cluster that needed no rebalancing.
    """
    before = sharding_metrics.distribution(client, namespace)
    before_bytes = sharding_metrics.owned_bytes(client, namespace)
    chunks_before = sum(sharding_metrics.chunk_counts(client, namespace).values())
    marker = sharding_metrics.move_marker(client)

    started = time.time()
    sharding_metrics.set_balancer(client, True)
    try:
        converge_s = sharding_metrics.wait_for_balancer(client, namespace)
    finally:
        # left off for whatever runs next: every other experiment measures with
        # a still cluster
        sharding_metrics.set_balancer(client, False)

    after = sharding_metrics.distribution(client, namespace)
    after_bytes = sharding_metrics.owned_bytes(client, namespace)
    moves = sharding_metrics.moves_since(client, marker)
    ## What the balancer could not move however long it ran. A cluster left
    ## unbalanced with zero migrations and a jumbo chunk is not a balancer
    ## that failed; it is a shard key that cannot be balanced.
    jumbo = sharding_metrics.oversized_chunks(
        client, namespace,
        {s1_row["shard_key_field"]:
            "hashed" if s1_row["shard_key_kind"] == "hashed" else 1},
        args.chunk_size_mb)

    return shard_runner.shard_row(
        experiment=EXPERIMENT, profile=args.profile, corpus=args.corpus,
        repetition=s1_row["repetition"], deployment=args.deployment,
        operation_mode=s1_row["operation_mode"],
        shard_key_field=s1_row["shard_key_field"],
        shard_key_kind=s1_row["shard_key_kind"],
        shard_key_role=s1_row["shard_key_role"],
        shard_key_cardinality=s1_row["shard_key_cardinality"],
        skew=s1_row["skew"], chunk_size_mb=args.chunk_size_mb,
        presplit_chunks=s1_row["presplit_chunks"], balancer="on",
        apply_s=round(time.time() - started, 3),
        docs_before=sum(before.values()), docs_after=sum(after.values()),
        dist_before=sharding_metrics.as_json(before),
        dist_after=sharding_metrics.as_json(after),
        imbalance_before=round(sharding_metrics.imbalance(before), 4),
        imbalance_after=round(sharding_metrics.imbalance(after), 4),
        chunks_before=chunks_before,
        chunks_after=sum(sharding_metrics.chunk_counts(client, namespace).values()),
        chunks_moved=len(moves), jumbo_chunks=jumbo,
        bytes_moved=sharding_metrics.arrived(before_bytes, after_bytes),
        orphans_after=sum(sharding_metrics.orphans(client, namespace).values()),
        converge_s=converge_s if converge_s is not None else "",
        records=s1_row["records"], mongo_version=mongo_version)
