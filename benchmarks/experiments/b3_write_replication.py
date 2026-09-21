"""B3 - Write cost of replication.

What does durability cost as the cluster grows? Insert-only workload swept
across write concerns and node counts. The corpus is rebuilt per repetition
because the workload mutates state.
"""

import sys

from benchmarks.bench_utils import effective_write_concern
from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import results, resume, runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import run_workload

EXPERIMENT = "b3"
OPERATION_MODE = "preprocess"
WRITE_CONCERNS = ["1", "majority", "all"]
CHAIN_LENGTH = 0

#: What makes one row of this experiment distinct from another. write_concern is
#: the token the row records ("all"), not the member count it resolves to, so the
#: key matches what a previous run actually wrote.
KEY_COLUMNS = ["experiment", "deployment", "corpus", "write_concern", "repetition"]


def cells(_deployment):
    """Every deployment sweeps the same write concerns.

    The unused argument keeps the cells() signature identical across all four
    experiment modules; only B1 and B4 vary their sweep by deployment.
    """
    return list(WRITE_CONCERNS)


def main(argv=None):
    args = runner.base_parser(EXPERIMENT).parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)

    primary, _ = runner.resolve_endpoints(args.deployment)
    nodes = topology.node_count(args.deployment)
    mongo_version = topology.server_version(primary)

    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    done = set() if args.fresh else resume.completed_cells(
        args.out, EXPERIMENT, args.profile, KEY_COLUMNS)
    written = skipped = 0

    for token in cells(args.deployment):
        concern = effective_write_concern(token, nodes)
        for repetition in range(profile["reps"]):
            # The workload inserts, so each repetition needs its own corpus. The
            # skip therefore goes before the build, not after it.
            if resume.cell_key(
                    {"experiment": EXPERIMENT, "deployment": args.deployment,
                     "corpus": args.corpus, "write_concern": token,
                     "repetition": repetition}, KEY_COLUMNS) in done:
                skipped += 1
                continue
            print(f"[{EXPERIMENT}] {args.deployment} / w={token} rep={repetition}",
                  flush=True)
            handle = corpus_module.build_corpus(
                args.corpus, primary_uri=primary, records=profile["records"],
                chain_length=CHAIN_LENGTH, operation_mode=OPERATION_MODE,
                write_concern=concern, max_files=profile["real_max_files"],
            )
            try:
                specs = runner.client_specs(
                    profile["clients"], "primary", "split",
                    OPERATION_MODE, concern, primary, [])
                result = run_workload(specs, handle, read_ratio=0.0,
                                      warmup_s=profile["warmup_s"],
                                      measure_s=profile["measure_s"])
                row = runner.row_from(
                    result, experiment=EXPERIMENT, profile_name=args.profile,
                    profile=profile, deployment=args.deployment,
                    corpus_kind=args.corpus, repetition=repetition,
                    corpus_handle=handle, mongo_version=mongo_version,
                    read_target="primary", read_mode="split",
                    operation_mode=OPERATION_MODE, write_concern=token,
                    chain_length=CHAIN_LENGTH, mix="write_only")
                runner.warn_if_noisy(row)
                results.write_rows(out_path, [row])
                written += 1
            finally:
                corpus_module.drop_corpus(primary, handle)

    print(f"[{EXPERIMENT}] wrote {written} rows, skipped {skipped} already done,"
          f" to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
