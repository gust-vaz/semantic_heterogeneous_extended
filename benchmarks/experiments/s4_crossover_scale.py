"""S4 - does the rewrite's advantage over preprocess grow with the shard count?

The campaign's first headline. `sh1` gives the intercept - a router, a config
server and sharded collections with nothing to distribute across - and `sh4`
and `sh8` give the slope, so the claim is about distribution rather than about
being behind a router. Comparing `sh4` to a plain standalone would change three
things at once.

The shard key is held at the realistic anchor: S1 and S3 are the experiments
that vary it. What varies here is strategy, workload mix and shard count.
"""

import sys

from pymongo import MongoClient

from benchmarks.harness import cell_setup, results, resume, sharding_metrics
from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import shard_runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import ClientSpec, run_workload
from benchmarks.experiments.s1_operation_cost import FIELDS

EXPERIMENT = "s4"
BALANCER = False
CHAIN_LENGTH = 5
WRITE_CONCERN = "majority"
SHARD_KEY = {"role": "data", "kind": "hashed"}
READ_RATIO = {"read_heavy": 0.95, "write_heavy": 0.05}
#: Fixed across repetitions, unlike S1. There the corpus is the variable - one
#: operation's relocation is a coin flip on one value's hash, so every
#: repetition must draw again. Here the corpus is a controlled condition and
#: the repetitions estimate run-to-run variance in throughput, which a changing
#: corpus would only add noise to.
SEED = 42

KEY_COLUMNS = ["experiment", "deployment", "corpus", "operation_mode", "mix",
               "repetition"]


def cells():
    """(operation_mode, mix) pairs. The same four at every deployment."""
    return [(mode, mix) for mode in ("preprocess", "rewrite")
            for mix in ("read_heavy", "write_heavy")]


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
    matrix_cell = next(entry for entry in cell_setup.SHARD_KEY_MATRIX
                       if entry["role"] == SHARD_KEY["role"]
                       and entry["kind"] == SHARD_KEY["kind"])
    written = skipped = 0

    cell_setup.prepare(client, args.chunk_size_mb, BALANCER)

    for mode, mix in cells():
        for repetition in range(profile["reps"]):
            key = resume.cell_key({
                "experiment": EXPERIMENT, "deployment": args.deployment,
                "corpus": args.corpus, "operation_mode": mode, "mix": mix,
                "repetition": repetition}, KEY_COLUMNS)
            if key in done:
                skipped += 1
                continue
            print(f"[{EXPERIMENT}] {args.deployment} / {mode} / {mix} "
                  f"rep={repetition}", flush=True)

            # Rebuilt per repetition: the workload inserts, so repetition 2
            # would otherwise start from a bigger collection than repetition 1.
            handle = corpus_module.build_corpus(
                args.corpus, primary_uri=uri, records=profile["records"],
                chain_length=CHAIN_LENGTH, operation_mode=mode,
                write_concern=WRITE_CONCERN,
                max_files=profile["real_max_files"], seed=SEED,
                shard_key=cell_setup.key_pattern(field, SHARD_KEY["kind"]))
            suffix = "_processed" if mode == "preprocess" else ""
            namespace = f"{handle.database_name}.{handle.collection_name}{suffix}"
            try:
                dist = sharding_metrics.distribution(client, namespace)
                cell_setup.assert_distribution(
                    dist, cell_setup.expects_spread(shard_count, matrix_cell),
                    f"{args.deployment}/{mode}/{mix}")

                specs = [ClientSpec(mongo_uri=uri, operation_mode=mode,
                                    write_concern=WRITE_CONCERN)
                         for _ in range(profile["clients"])]
                result = run_workload(specs, handle,
                                      read_ratio=READ_RATIO[mix],
                                      warmup_s=profile["warmup_s"],
                                      measure_s=profile["measure_s"])
                row = shard_runner.shard_row(
                    experiment=EXPERIMENT, profile=args.profile,
                    corpus=args.corpus, repetition=repetition,
                    deployment=args.deployment, operation_mode=mode,
                    shard_key_field=field, shard_key_kind=SHARD_KEY["kind"],
                    shard_key_role=SHARD_KEY["role"],
                    chunk_size_mb=args.chunk_size_mb, balancer="off", mix=mix,
                    dist_before=sharding_metrics.as_json(dist),
                    imbalance_before=round(sharding_metrics.imbalance(dist), 4),
                    clients=profile["clients"], records=handle.record_count,
                    chain_length=CHAIN_LENGTH,
                    warmup_s=profile["warmup_s"], measure_s=profile["measure_s"],
                    # where preprocess pays and rewrite does not
                    setup_insert_s=round(handle.setup_insert_s, 3),
                    setup_operations_s=round(handle.setup_operations_s, 3),
                    mongo_version=mongo_version,
                    **shard_runner.summarize(result))
                shard_runner.emit(out_path, row)
                written += 1
            finally:
                corpus_module.drop_corpus(uri, handle)

    print(f"[{EXPERIMENT}] wrote {written} rows, skipped {skipped} already done,"
          f" to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
