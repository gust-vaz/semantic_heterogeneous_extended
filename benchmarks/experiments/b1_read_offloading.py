"""B1 - Read offloading throughput.

Does routing record reads to secondaries raise aggregate query throughput
without moving the version chain off the primary? Workers are assigned nodes
round-robin so reads genuinely fan out instead of piling onto one secondary.
"""

import sys

from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import results, runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import run_workload

EXPERIMENT = "b1"
OPERATION_MODE = "preprocess"
WRITE_CONCERN = "majority"
CHAIN_LENGTH = 5


def cells(deployment):
    return runner.read_targets_for(deployment)


def main(argv=None):
    args = runner.base_parser(EXPERIMENT).parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)

    primary, secondaries = runner.resolve_endpoints(args.deployment)
    mongo_version = topology.server_version(primary)

    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    rows = []

    for read_target in cells(args.deployment):
        print(f"[{EXPERIMENT}] {args.deployment} / {read_target}", flush=True)
        runner.warn_if_target_unavailable(read_target, secondaries)
        handle = corpus_module.build_corpus(
            args.corpus, primary_uri=primary, records=profile["records"],
            chain_length=CHAIN_LENGTH, operation_mode=OPERATION_MODE,
            write_concern=WRITE_CONCERN, max_files=profile["real_max_files"],
        )
        try:
            specs = runner.client_specs(
                profile["clients"], read_target, "split",
                OPERATION_MODE, WRITE_CONCERN, primary, secondaries)
            for repetition in range(profile["reps"]):
                result = run_workload(specs, handle, read_ratio=1.0,
                                      warmup_s=profile["warmup_s"],
                                      measure_s=profile["measure_s"])
                row = runner.row_from(
                    result, experiment=EXPERIMENT, profile_name=args.profile,
                    profile=profile, deployment=args.deployment,
                    corpus_kind=args.corpus, repetition=repetition,
                    corpus_handle=handle, mongo_version=mongo_version,
                    read_target=read_target,
                    read_mode="single_source" if read_target.endswith("single_source") else "split",
                    operation_mode=OPERATION_MODE, write_concern=WRITE_CONCERN,
                    chain_length=CHAIN_LENGTH, mix="read_only")
                runner.warn_if_noisy(row)
                rows.append(row)
        finally:
            corpus_module.drop_corpus(primary, handle)

    results.write_rows(out_path, rows)
    print(f"[{EXPERIMENT}] wrote {len(rows)} rows to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
