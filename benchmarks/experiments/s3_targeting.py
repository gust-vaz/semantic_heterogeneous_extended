"""S3 - what naming the shard key in a filter is worth, and what it costs not to.

The Phase 2 spike proved that the nested $or MellowDB's rewrite generates is
targetable, with four queries and no load. This puts load behind it: a targeted
query should get faster as shards are added, and a broadcast one slower,
because the fan-out grows with the shard count.

The two ends of the filter axis share a corpus - same collection, different
queries - so neither the axis nor the repetitions cost any extra setup. That
makes S3 the cheapest experiment in the series by a wide margin.
"""

import sys
from dataclasses import replace

from pymongo import MongoClient

from benchmarks.harness import cell_setup, results, resume, sharding_metrics
from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import shard_runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import ClientSpec, run_workload
from benchmarks.experiments.s1_operation_cost import (
    FIELDS, chunks_needed, split_points,
)

EXPERIMENT = "s3"
BALANCER = False
CHAIN_LENGTH = 5
WRITE_CONCERN = "majority"
#: The realistic production choice: a data field the operations never touch.
ANCHOR = {"role": "data", "kind": "hashed"}

KEY_COLUMNS = ["experiment", "deployment", "corpus", "operation_mode",
               "shard_key_role", "shard_key_kind", "mix", "repetition"]


def evolved_fields(client, handle):
    """Which fields this corpus's semantic operations touch.

    Read from the version chain, so it holds for the DATASUS corpus as much as
    for a synthetic one. MellowDB's `_columns` collection is empty on a corpus
    built this way and cannot serve.
    """
    versions = client[handle.database_name][handle.collection_name + "_versions"]
    fields = set()
    for document in versions.find({}, {"next_operation.field": 1}):
        operation = document.get("next_operation") or {}
        if operation.get("field"):
            fields.add(operation["field"])
    return fields


def query_sets(handle, field, evolved=frozenset()):
    """The corpus's query set split into a targeted arm and a broadcast one.

    The broadcast arm is drawn only from fields of the same class as the shard
    key - evolved if the key evolves, plain otherwise - so the two arms differ
    in targeting and nothing else.

    Without that restriction the evolved row compares two things at once: a
    query naming an evolved field also pays for expanding the version chain,
    and the first measured run had its targeted arm come out SLOWER than its
    broadcast one (252 against 298 ops/s) for exactly that reason.
    """
    if not handle.query_set:
        raise RuntimeError("the corpus produced no queries to run")
    key_evolves = field in evolved

    def same_class(query):
        return all((name in evolved) == key_evolves for name in query)

    return {
        "targeted": [query for query in handle.query_set if field in query],
        "broadcast": [query for query in handle.query_set
                      if field not in query and same_class(query)],
    }


def arms(sets):
    """Which ends of the filter axis this cell can actually run.

    A cell sharded on `_id` has no targeted arm: MellowDB's queries name data
    fields, never the system id, so every semantic query it serves is a
    broadcast. That is the row's result rather than a cell to skip.
    """
    return [name for name in ("targeted", "broadcast") if sets[name]]


def cells(deployment):
    """(role, kind, operation_mode) per cell: the six keys, plus rewrite at the anchor."""
    out = [(cell["role"], cell["kind"], "preprocess")
           for cell in cell_setup.SHARD_KEY_MATRIX]
    out.append((ANCHOR["role"], ANCHOR["kind"], "rewrite"))
    return out


def namespace_for(handle, operation_mode):
    """Which collection holds the records in this mode."""
    suffix = "_processed" if operation_mode == "preprocess" else ""
    return f"{handle.database_name}.{handle.collection_name}{suffix}"


def main(argv=None):
    args = shard_runner.base_parser(EXPERIMENT).parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)
    uri = shard_runner.router_uri(args.deployment)
    client = MongoClient(uri)
    mongo_version = topology.server_version(uri)
    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    done = shard_runner.already_done(args, EXPERIMENT, KEY_COLUMNS)
    shard_count = topology.shard_count(args.deployment)
    written = skipped = 0

    cell_setup.prepare(client, args.chunk_size_mb, BALANCER)

    for role, kind, mode in cells(args.deployment):
        matrix_cell = next(entry for entry in cell_setup.SHARD_KEY_MATRIX
                           if entry["role"] == role and entry["kind"] == kind)
        field = FIELDS[role]
        wanted = [(mix, repetition)
                  for mix in ("targeted", "broadcast")
                  for repetition in range(profile["reps"])
                  if resume.cell_key({
                      "experiment": EXPERIMENT, "deployment": args.deployment,
                      "corpus": args.corpus, "operation_mode": mode,
                      "shard_key_role": role, "shard_key_kind": kind,
                      "mix": mix, "repetition": repetition},
                      KEY_COLUMNS) not in done]
        if not wanted:
            print(f"[{EXPERIMENT}] {args.deployment} / {role}-{kind} / {mode}:"
                  f" all done, skipping", flush=True)
            skipped += 2 * profile["reps"]
            continue

        print(f"[{EXPERIMENT}] {args.deployment} / {role}-{kind} / {mode}",
              flush=True)
        # Read-only: one corpus serves both arms and every repetition.
        handle = corpus_module.build_corpus(
            args.corpus, primary_uri=uri, records=profile["records"],
            chain_length=CHAIN_LENGTH, operation_mode=mode,
            write_concern=WRITE_CONCERN, max_files=profile["real_max_files"],
            shard_key=cell_setup.key_pattern(field, kind))
        namespace = namespace_for(handle, mode)
        try:
            shards = sorted(entry["_id"] for entry in client["config"].shards.find())
            if kind == "ranged" and role != "id" and len(shards) > 1:
                cell_setup.presplit(
                    client, namespace, field,
                    split_points(client, handle, field,
                                 chunks_needed(client, namespace,
                                               args.chunk_size_mb, len(shards))),
                    shards)

            dist = sharding_metrics.distribution(client, namespace)
            cell_setup.assert_distribution(
                dist, cell_setup.expects_spread(shard_count, matrix_cell),
                f"{args.deployment}/{role}-{kind}/{mode}")

            sets = query_sets(handle, field, evolved_fields(client, handle))
            runnable = arms(sets)
            specs = [ClientSpec(mongo_uri=uri, operation_mode=mode,
                                write_concern=WRITE_CONCERN)
                     for _ in range(profile["clients"])]

            for mix, repetition in wanted:
                if mix not in runnable:
                    ## Sharding on _id leaves no query that names the key.
                    skipped += 1
                    continue
                touched = sharding_metrics.matched_by_shard(
                    client, namespace, sets[mix][0])
                result = run_workload(specs, replace(handle, query_set=sets[mix]),
                                      read_ratio=1.0,
                                      warmup_s=profile["warmup_s"],
                                      measure_s=profile["measure_s"])
                row = shard_runner.shard_row(
                    experiment=EXPERIMENT, profile=args.profile,
                    corpus=args.corpus, repetition=repetition,
                    deployment=args.deployment, operation_mode=mode,
                    shard_key_field=field, shard_key_kind=kind,
                    shard_key_role=role, chunk_size_mb=args.chunk_size_mb,
                    balancer="off", mix=mix,
                    shards_touched=len(touched),
                    dist_before=sharding_metrics.as_json(dist),
                    imbalance_before=round(sharding_metrics.imbalance(dist), 4),
                    clients=profile["clients"], records=handle.record_count,
                    chain_length=CHAIN_LENGTH,
                    warmup_s=profile["warmup_s"], measure_s=profile["measure_s"],
                    setup_insert_s=round(handle.setup_insert_s, 3),
                    setup_operations_s=round(handle.setup_operations_s, 3),
                    mongo_version=mongo_version,
                    **shard_runner.summarize(result))
                shard_runner.emit(out_path, row)
                written += 1
        finally:
            corpus_module.drop_corpus(uri, handle)

    print(f"[{EXPERIMENT}] wrote {written} rows, skipped {skipped}, "
          f"to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
