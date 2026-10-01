"""S5 - what MellowDB's schemaless promise costs on a sharded cluster.

A document that lacks the shard key field cannot go through `$merge`: the
server rejects it with Location51132, and it does so after other shards have
already written their copies. MellowDB therefore copies those documents through
the driver, in batches of Collection._CLIENT_SPLIT_BATCH - a round trip per
batch instead of work that stays on the server.

That is a deliberate design decision of the library, taken in Phase 2 to keep
it schemaless. This measures its price rather than leaving it implied.
"""

import sys
import time
from datetime import datetime

from pymongo import MongoClient

from benchmarks.harness import cell_setup, results, resume, sharding_metrics
from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import shard_runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.experiments.s1_operation_cost import (
    FIELDS, open_collection, pick_most_frequent_value,
)

EXPERIMENT = "s5"
BALANCER = False
CHAIN_LENGTH = 0
WRITE_CONCERN = "majority"
SKEW = 3.0
#: The shard key must be a field the operation never touches: the only row of
#: the matrix where a document can legitimately lack the key. Every document
#: has an `_id`, and the evolving field is the one the operation needs.
SHARD_KEY = {"role": "data", "kind": "hashed"}
MISSING_FRACTIONS = [0.0, 0.25, 0.5, 1.0]
#: Fixed: the fraction is the axis, so every cell must start from the same
#: corpus or the comparison moves twice at once.
SEED = 42

KEY_COLUMNS = ["experiment", "deployment", "corpus", "operation_mode",
               "missing_fraction", "repetition"]


#: How many documents to rewrite per round trip while stripping.
STRIP_BATCH = 1000


def strip_shard_key(collection, field, fraction, batch=STRIP_BATCH):
    """Remove `field` from `fraction` of the documents. Returns how many.

    Done by deleting and re-inserting without the field, not by `$unset`.
    MongoDB refuses to unset a shard key value in bulk - "Multi-update
    operations are not allowed when updating the shard key field" (72) -
    because removing it changes where the document routes, and refuses a
    single-document update that does not name the full shard key (31025).
    Naming it per document does work, and is 240x slower: measured at 7.22 s
    against 0.03 s for 500 documents, which at campaign sizes would be minutes
    of setup per cell.

    Documents are chosen by sorted `_id` rather than at random, so the same
    fraction removes the same documents on every repetition and the axis moves
    alone. Ids are collected before any write, so no cursor is read while the
    collection underneath it changes.
    """
    total = collection.count_documents({})
    wanted = int(total * fraction)
    if wanted == 0:
        return 0
    ids = [document["_id"] for document
           in collection.find({}, {"_id": 1}).sort("_id", 1).limit(wanted)]
    for start in range(0, len(ids), batch):
        window = ids[start:start + batch]
        documents = list(collection.find({"_id": {"$in": window}}))
        collection.delete_many({"_id": {"$in": window}})
        for document in documents:
            document.pop(field, None)
        collection.insert_many(documents)
    return len(ids)


def main(argv=None):
    args = shard_runner.base_parser(EXPERIMENT).parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)
    uri = shard_runner.router_uri(args.deployment)
    client = MongoClient(uri)
    mongo_version = topology.server_version(uri)
    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    done = shard_runner.already_done(args, EXPERIMENT, KEY_COLUMNS)
    shard_count = topology.shard_count(args.deployment)
    field = FIELDS[SHARD_KEY["role"]]
    evolving = FIELDS["evolved"]
    written = skipped = failed = 0

    cell_setup.prepare(client, args.chunk_size_mb, BALANCER)

    for fraction in MISSING_FRACTIONS:
        for repetition in range(profile["reps"]):
            key = resume.cell_key({
                "experiment": EXPERIMENT, "deployment": args.deployment,
                "corpus": args.corpus, "operation_mode": "preprocess",
                "missing_fraction": fraction, "repetition": repetition},
                KEY_COLUMNS)
            if key in done:
                skipped += 1
                continue
            print(f"[{EXPERIMENT}] {args.deployment} / missing={fraction} "
                  f"rep={repetition}", flush=True)

            handle = corpus_module.build_corpus(
                args.corpus, primary_uri=uri, records=profile["records"],
                chain_length=CHAIN_LENGTH, operation_mode="preprocess",
                write_concern=WRITE_CONCERN,
                max_files=profile["real_max_files"], seed=SEED, skew=SKEW,
                shard_key=cell_setup.key_pattern(field, SHARD_KEY["kind"]))
            namespace = f"{handle.database_name}.{handle.collection_name}_processed"
            processed = client[handle.database_name][
                handle.collection_name + "_processed"]
            try:
                # Only the processed collection: that is the one preprocess
                # materialises into, and whose split path this measures.
                strip_shard_key(processed, field, fraction)

                before = sharding_metrics.distribution(client, namespace)
                # At fraction 1.0 every document sorts under null and lands on
                # one chunk. That is the result, not a misconfigured cell.
                cell_setup.assert_distribution(
                    before, shard_count > 1 and fraction < 1.0,
                    f"{args.deployment}/missing-{fraction}")

                old_value = pick_most_frequent_value(processed, evolving)
                collection = open_collection(handle, uri)
                started = time.time()
                collection.execute_operation(
                    "translation", datetime(2021, 1, 1),
                    {"fieldName": evolving, "oldValue": old_value,
                     "newValue": f"{old_value}__evolved"})
                apply_s = time.time() - started
                after = sharding_metrics.distribution(client, namespace)

                row = shard_runner.shard_row(
                    experiment=EXPERIMENT, profile=args.profile,
                    corpus=args.corpus, repetition=repetition,
                    deployment=args.deployment, operation_mode="preprocess",
                    shard_key_field=field, shard_key_kind=SHARD_KEY["kind"],
                    shard_key_role=SHARD_KEY["role"], skew=SKEW,
                    missing_fraction=fraction, chunk_size_mb=args.chunk_size_mb,
                    balancer="off", apply_s=round(apply_s, 3),
                    docs_before=sum(before.values()),
                    docs_after=sum(after.values()),
                    docs_written=sum(after.values()) - sum(before.values()),
                    dist_before=sharding_metrics.as_json(before),
                    dist_after=sharding_metrics.as_json(after),
                    imbalance_before=round(sharding_metrics.imbalance(before), 4),
                    imbalance_after=round(sharding_metrics.imbalance(after), 4),
                    records=handle.record_count,
                    setup_insert_s=round(handle.setup_insert_s, 3),
                    setup_operations_s=round(handle.setup_operations_s, 3),
                    mongo_version=mongo_version)
                shard_runner.emit(out_path, row)
                written += 1
            except RuntimeError as exc:
                ## The pre-flight guard refuses a cell it cannot measure. Skip it
                ## and keep the campaign going rather than aborting the rest.
                print(f"[{EXPERIMENT}] skipped (guard): {exc}", flush=True)
                failed += 1
                continue
            finally:
                corpus_module.drop_corpus(uri, handle)

    print(f"[{EXPERIMENT}] wrote {written} rows, skipped {skipped} already done,"
          f" {failed} failed the guard,"
          f" to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
