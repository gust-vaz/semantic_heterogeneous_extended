"""B1 - Read offloading throughput.

Does routing record reads to secondaries raise aggregate query throughput
without moving the version chain off the primary? Workers are assigned nodes
round-robin so reads genuinely fan out instead of piling onto one secondary.
"""

import sys

from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import results, resume, runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import run_workload

EXPERIMENT = "b1"
OPERATION_MODE = "preprocess"
WRITE_CONCERN = "majority"
CHAIN_LENGTH = 5

#: What makes one row of this experiment distinct from another. A campaign is
#: resumed against these, so every axis the sweep varies has to appear here.
KEY_COLUMNS = ["experiment", "deployment", "corpus", "read_target", "repetition"]


def cells(deployment):
    return runner.read_targets_for(deployment)


def main(argv=None):
    args = runner.base_parser(EXPERIMENT).parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)

    primary, secondaries = runner.resolve_endpoints(args.deployment)
    mongo_version = topology.server_version(primary)

    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    done = set() if args.fresh else resume.completed_cells(
        args.out, EXPERIMENT, args.profile, KEY_COLUMNS)
    written = skipped = 0

    for read_target in cells(args.deployment):
        # The corpus is read-only here, so it is built once and every repetition
        # reuses it. Which repetitions are still pending therefore has to be
        # known before paying for the build, not inside the loop.
        pending = [repetition for repetition in range(profile["reps"])
                   if resume.cell_key(
                       {"experiment": EXPERIMENT, "deployment": args.deployment,
                        "corpus": args.corpus, "read_target": read_target,
                        "repetition": repetition}, KEY_COLUMNS) not in done]
        skipped += profile["reps"] - len(pending)
        if not pending:
            print(f"[{EXPERIMENT}] {args.deployment} / {read_target}: "
                  f"all reps done, skipping", flush=True)
            continue

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
            for repetition in pending:
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
                # written as it is measured: an interrupted campaign keeps
                # everything it finished
                results.write_rows(out_path, [row])
                written += 1
        finally:
            corpus_module.drop_corpus(primary, handle)

    print(f"[{EXPERIMENT}] wrote {written} rows, skipped {skipped} already done,"
          f" to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
