"""S1 - what one semantic operation costs on a cluster, and how much data it moves.

The headline of the sharding campaign's second claim: in a semantically
evolving database the evolution itself relocates data, so the shard key becomes
a problem nobody else has.

The measurement is the operation apply, which costs under a second; building
the corpus around it costs minutes. Every saving here is a saving in setup,
which is why S2 rides along on the same corpus instead of building its own.
"""

import sys
import time
from datetime import datetime

from pymongo import MongoClient

from benchmarks.experiments import s2_rebalance
from benchmarks.harness import cell_setup, results, resume, sharding_metrics
from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import shard_runner, topology
from benchmarks.harness.profiles import get_profile

EXPERIMENT = "s1"
BALANCER = False
#: No chain: the corpus is built clean and this experiment applies exactly one
#: operation, on whichever value the corpus has most of.
CHAIN_LENGTH = 0
WRITE_CONCERN = "majority"
OPERATION_MODE = "preprocess"
#: Each repetition draws a different corpus. A translation moves every copy to
#: the one shard its new value routes to, so whether anything relocated is a
#: single coin flip on one string's hash - and with a fixed seed that flip comes
#: out the same every time. Measured: seed 42 twice gave the same dominant
#: value, the same origin and the same zero; seeds 7 and 5 relocated 826 and 839
#: documents while 13 and 99 relocated none. Repetitions that do not change the
#: draw are not repetitions.
BASE_SEED = 42

#: Skew is the axis that moves how much of the collection one operation
#: touches. Measured on a 2000-record corpus with a 20-value domain, the most
#: frequent value covers 6.1% at skew 0.0, 27.8% at 1.0, 46.0% at 1.5 and 83.2%
#: at 3.0 - the last being the range DATASUS's `cid` sits in (80.04%). Coverage
#: is therefore not a separate axis: asking a uniform corpus for 80% returns
#: whatever its largest value holds, which is about 1/cardinality.
SKEW_LEVELS = [0.0, 1.5, 3.0]
CARDINALITY_LEVELS = [20, 200, 2000]
ANCHOR = {"skew": 3.0, "cardinality": 20}

#: Which field each matrix role points at. `evolved` is the field the operation
#: rewrites; `data` is one it never touches, which is the realistic production
#: choice and the only row where a document can legitimately lack the key.
FIELDS = {"id": "_id", "data": "f0", "evolved": "evo0"}

KEY_COLUMNS = ["experiment", "deployment", "corpus", "operation_mode",
               "shard_key_role", "shard_key_kind", "skew",
               "shard_key_cardinality", "repetition"]


def pick_most_frequent_value(collection, field):
    """The value the operation will evolve: whichever the corpus has most of.

    Not "the value closest to a requested coverage". A synthetic corpus cannot
    honour such a request - every value covers about 1/cardinality of it, and
    skew is what moves that - so a requested figure would end up recorded
    beside an operation that touched something else entirely. How much this
    operation really touched is docs_written / docs_before, which the row
    already carries.
    """
    counts = {row["_id"]: row["n"] for row in collection.aggregate(
        [{"$group": {"_id": f"${field}", "n": {"$sum": 1}}}])}
    return max(counts, key=counts.get)


def cells(deployment, anchor_only=False):
    """The cells this deployment runs: the core, plus OFAT extensions on sh4.

    The core is the whole shard key matrix, which crossed with the deployments
    gives the shard-key x scale plane both halves of the headline need. The
    other three axes vary one at a time from the anchor, because a full
    factorial would be 216 cells and the setup, not the measurement, is what
    costs.
    """
    core = [dict(ANCHOR, role=cell["role"], kind=cell["kind"],
                 negative_control=cell["negative_control"])
            for cell in cell_setup.SHARD_KEY_MATRIX]
    if anchor_only or deployment != "sh4":
        return core

    extra = []
    for skew in SKEW_LEVELS:
        if skew != ANCHOR["skew"]:
            ## Applies to every key, not just the ranged ones: skew changes how
            ## much of the collection the operation touches, which every shard
            ## key feels.
            extra += [dict(cell, skew=skew) for cell in core]
    for cardinality in CARDINALITY_LEVELS:
        if cardinality != ANCHOR["cardinality"]:
            ## Cardinality is a property of the evolving field's domain, and
            ## semantic fields are low-cardinality by nature: they are codes.
            extra += [dict(cell, cardinality=cardinality) for cell in core
                      if cell["role"] == "evolved"]
    return core + extra


def chunks_needed(client, namespace, chunk_size_mb, shard_count):
    """How many chunks this collection needs for a balanced layout to be movable.

    One chunk per shard is not enough beyond a few megabytes: MongoDB refuses
    to move a chunk larger than the chunk size, so a 7 MB collection split into
    four 1.75 MB chunks is immovable even when perfectly distributed. Sizing
    the split by volume keeps that artefact out of the jumbo count, leaving
    only the chunks a dominant shard key value really does force.
    """
    total = sum(sharding_metrics.owned_bytes(client, namespace).values())
    limit = chunk_size_mb * 1024 * 1024
    return max(shard_count, -(-total // limit))


def split_points(client, handle, field, chunks):
    """chunks - 1 values from the field's own domain, evenly spaced.

    Drawn from the corpus rather than computed from a numeric range: shard key
    fields are strings as often as numbers, and only the data knows its order.

    A boundary can only fall between distinct values, so a low-cardinality
    field caps how finely the collection can be cut. Asking for more chunks
    than the domain allows yields as many as it does - and whatever stays
    oversized after that is the finding, not a setup error.
    """
    raw = client[handle.database_name][handle.collection_name]
    values = sorted(raw.distinct(field))
    chunks = min(chunks, len(values))
    if chunks < 2:
        raise RuntimeError(
            f"field '{field}' has {len(values)} distinct values, too few to "
            f"pre-split at all")
    step = len(values) // chunks
    return [values[step * index] for index in range(1, chunks)]


def open_collection(handle, uri):
    """A BasicCollection over a corpus that already exists.

    No shard_key is passed: sharding.ensure discovers the key already in effect
    and returns it, rather than re-sharding or complaining.
    """
    from semantic_heterogeneous_database import BasicCollection

    return BasicCollection(handle.database_name, handle.collection_name,
                           uri, OPERATION_MODE, write_concern=WRITE_CONCERN)


def measure_cell(client, args, profile, cell, repetition, mongo_version):
    """Build one cell's corpus, apply one operation, and measure what moved.

    Returns (row, handle). The handle comes back because S2 continues from this
    corpus rather than paying for its own.
    """
    uri = shard_runner.router_uri(args.deployment)
    field = FIELDS[cell["role"]]
    shard_count = topology.shard_count(args.deployment)
    label = (f"{args.deployment}/{cell['role']}-{cell['kind']}"
             f"/skew{cell['skew']}/card{cell['cardinality']}")

    cell_setup.prepare(client, args.chunk_size_mb, BALANCER)
    handle = corpus_module.build_corpus(
        args.corpus, primary_uri=uri, records=profile["records"],
        chain_length=CHAIN_LENGTH, operation_mode=OPERATION_MODE,
        write_concern=WRITE_CONCERN, max_files=profile["real_max_files"],
        shard_key=cell_setup.key_pattern(field, cell["kind"]),
        skew=cell["skew"], domain=cell["cardinality"],
        seed=BASE_SEED + repetition)
    namespace = f"{handle.database_name}.{handle.collection_name}_processed"

    try:
        shards = sorted(entry["_id"] for entry in client["config"].shards.find())
        presplit_chunks = 0
        if cell["kind"] == "ranged" and cell["role"] != "id" and len(shards) > 1:
            wanted = chunks_needed(client, namespace, args.chunk_size_mb,
                                   len(shards))
            presplit_chunks = cell_setup.presplit(
                client, namespace, field,
                split_points(client, handle, field, wanted), shards)

        before = sharding_metrics.distribution(client, namespace)
        pattern = cell_setup.key_pattern(field, cell["kind"])
        jumbo = sharding_metrics.oversized_chunks(
            client, namespace, pattern, args.chunk_size_mb)
        cell_setup.assert_distribution(
            before, cell_setup.expects_spread(shard_count, cell), label)

        processed = client[handle.database_name][
            handle.collection_name + "_processed"]
        evolving = FIELDS["evolved"]
        old_value = pick_most_frequent_value(processed, evolving)
        originals = sharding_metrics.matched_by_shard(
            client, namespace, {evolving: old_value})

        collection = open_collection(handle, uri)
        started = time.time()
        collection.execute_operation(
            "translation", datetime(2021, 1, 1),
            {"fieldName": evolving, "oldValue": old_value,
             "newValue": f"{old_value}__evolved"})
        apply_s = time.time() - started

        after = sharding_metrics.distribution(client, namespace)
        row = shard_runner.shard_row(
            experiment=EXPERIMENT, profile=args.profile, corpus=args.corpus,
            repetition=repetition, deployment=args.deployment,
            operation_mode=OPERATION_MODE,
            shard_key_field=field, shard_key_kind=cell["kind"],
            shard_key_role=cell["role"],
            shard_key_cardinality=cell["cardinality"], skew=cell["skew"],
            chunk_size_mb=args.chunk_size_mb,
            presplit_chunks=presplit_chunks, jumbo_chunks=jumbo,
            balancer="off", apply_s=round(apply_s, 3),
            docs_before=sum(before.values()), docs_after=sum(after.values()),
            docs_written=sum(after.values()) - sum(before.values()),
            docs_relocated=sharding_metrics.relocated(before, after, originals),
            dist_before=sharding_metrics.as_json(before),
            dist_after=sharding_metrics.as_json(after),
            imbalance_before=round(sharding_metrics.imbalance(before), 4),
            imbalance_after=round(sharding_metrics.imbalance(after), 4),
            records=handle.record_count,
            setup_insert_s=round(handle.setup_insert_s, 3),
            setup_operations_s=round(handle.setup_operations_s, 3),
            mongo_version=mongo_version)
        return row, handle
    except Exception:
        corpus_module.drop_corpus(uri, handle)
        raise


def main(argv=None):
    args = shard_runner.base_parser(EXPERIMENT).parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)
    uri = shard_runner.router_uri(args.deployment)
    client = MongoClient(uri)
    mongo_version = topology.server_version(uri)
    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    done = shard_runner.already_done(args, EXPERIMENT, KEY_COLUMNS)
    anchor = cells(args.deployment, anchor_only=True)
    written = skipped = failed = 0

    for cell in cells(args.deployment):
        for repetition in range(profile["reps"]):
            key = resume.cell_key({
                "experiment": EXPERIMENT, "deployment": args.deployment,
                "corpus": args.corpus, "operation_mode": OPERATION_MODE,
                "shard_key_role": cell["role"], "shard_key_kind": cell["kind"],
                "skew": cell["skew"],
                "shard_key_cardinality": cell["cardinality"],
                "repetition": repetition}, KEY_COLUMNS)
            if key in done:
                skipped += 1
                continue
            print(f"[{EXPERIMENT}] {args.deployment} / {cell['role']}-"
                  f"{cell['kind']} skew={cell['skew']} "
                  f"card={cell['cardinality']} rep={repetition}", flush=True)
            try:
                row, handle = measure_cell(client, args, profile, cell,
                                           repetition, mongo_version)
            except RuntimeError as exc:
                ## The pre-flight guard refuses a cell it cannot measure - a
                ## corpus that would not spread, say. Skip it and keep the
                ## campaign going rather than letting one bad cell abort every
                ## cell after it; measure_cell has already dropped its corpus.
                print(f"[{EXPERIMENT}] skipped (guard): {exc}", flush=True)
                failed += 1
                continue
            try:
                shard_runner.emit(out_path, row)
                written += 1
                if cell in anchor:
                    ## S2 rides along on this corpus instead of building its
                    ## own: setup is 99% of a cell's cost, and the state this
                    ## operation just left behind is what S2 wants to measure.
                    namespace = (f"{handle.database_name}."
                                 f"{handle.collection_name}_processed")
                    shard_runner.emit(
                        results.result_path(args.out, s2_rebalance.EXPERIMENT,
                                            args.profile),
                        s2_rebalance.rebalance_row(client, args, row,
                                                   namespace, mongo_version))
            finally:
                corpus_module.drop_corpus(uri, handle)

    print(f"[{EXPERIMENT}] wrote {written} rows, skipped {skipped} already done,"
          f" {failed} failed the guard, to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
