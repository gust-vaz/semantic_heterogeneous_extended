"""B6 - Version-chain depth.

How does query cost grow as semantic operations stack up, and does the strategy
change the shape of that growth? Read-only workload over corpora built with
increasing chain lengths.
"""

import sys

from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import results, runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import run_workload

EXPERIMENT = "b6"
CHAIN_LENGTHS = [1, 5, 10, 25, 50]
OPERATION_MODES = ["preprocess", "rewrite"]
WRITE_CONCERN = "majority"


def cells(_deployment, chain_lengths=None):
    """Cross product of chain length and strategy.

    The unused deployment argument keeps the cells() signature identical across
    all four experiment modules; only B1 and B4 vary their sweep by deployment.
    """
    lengths = chain_lengths or CHAIN_LENGTHS
    return [(length, mode) for length in lengths for mode in OPERATION_MODES]


def main(argv=None):
    parser = runner.base_parser(EXPERIMENT)
    parser.add_argument("--chain-lengths", default=None,
                        help="Comma-separated chain lengths, e.g. 1,5,10")
    args = parser.parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)

    lengths = ([int(v) for v in args.chain_lengths.split(",")]
               if args.chain_lengths else CHAIN_LENGTHS)

    # One domain for the whole sweep, sized for the longest chain. If the domain
    # grew with chain_length, query selectivity would change alongside it and
    # confound the very thing this experiment measures.
    domain = corpus_module.domain_for_chain(max(lengths))

    primary, _ = runner.resolve_endpoints(args.deployment)
    mongo_version = topology.server_version(primary)

    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    rows = []

    for chain_length, operation_mode in cells(args.deployment, lengths):
        print(f"[{EXPERIMENT}] {args.deployment} / chain={chain_length} "
              f"/ {operation_mode}", flush=True)
        handle = corpus_module.build_corpus(
            args.corpus, primary_uri=primary, records=profile["records"],
            chain_length=chain_length, operation_mode=operation_mode,
            write_concern=WRITE_CONCERN, max_files=profile["real_max_files"],
            domain=domain,
        )
        try:
            specs = runner.client_specs(
                profile["clients"], "primary", "split",
                operation_mode, WRITE_CONCERN, primary, [])
            for repetition in range(profile["reps"]):
                result = run_workload(specs, handle, read_ratio=1.0,
                                      warmup_s=profile["warmup_s"],
                                      measure_s=profile["measure_s"])
                row = runner.row_from(
                    result, experiment=EXPERIMENT, profile_name=args.profile,
                    profile=profile, deployment=args.deployment,
                    corpus_kind=args.corpus, repetition=repetition,
                    corpus_handle=handle, mongo_version=mongo_version,
                    read_target="primary", read_mode="split",
                    operation_mode=operation_mode, write_concern=WRITE_CONCERN,
                    chain_length=chain_length, mix="read_only")
                runner.warn_if_noisy(row)
                rows.append(row)
        finally:
            corpus_module.drop_corpus(primary, handle)

    results.write_rows(out_path, rows)
    print(f"[{EXPERIMENT}] wrote {len(rows)} rows to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
