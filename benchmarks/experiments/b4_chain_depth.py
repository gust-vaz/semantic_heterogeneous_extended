"""B4 - Version-chain depth.

How does query cost grow as semantic operations stack up, and does the strategy
change the shape of that growth? Read-only workload over corpora built with
increasing chain lengths.

The synthetic corpus is chained: one linear translation chain v0 -> ... -> vN,
with every query on a chain value, so resolving a query follows all N hops. The
default corpus would not do: its operations never share a value, so a query
goes through at most one of them and "depth" would only be operation count.
With two or more chained translations, preprocess stops finding the original
value (a limitation the library documents in its test_chained.py), so the two
strategies do not return the same records here - compare their costs knowing
that.
"""

import sys

from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import results, resume, runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import run_workload

EXPERIMENT = "b4"
#: Capped at 25: on the chained corpus preprocess cascades (each hop copies the
#: records earlier hops copied), so building c=50 does not finish at campaign
#: sizes - measured at 20k records, c=10 took 120 s and c=50 over 20 min.
CHAIN_LENGTHS = [1, 5, 10, 25]
OPERATION_MODES = ["preprocess", "rewrite"]
WRITE_CONCERN = "majority"

#: What makes one row of this experiment distinct from another. A campaign is
#: resumed against these, so every axis the sweep varies has to appear here.
KEY_COLUMNS = ["experiment", "deployment", "corpus", "operation_mode",
               "chain_length", "repetition"]


def cells(_deployment, chain_lengths=None):
    """Cross product of chain length and strategy.

    The unused deployment argument keeps the cells() signature identical across
    all four experiment modules; only B1 and B3 vary their sweep by deployment.
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

    # Rows name the corpus they measured, so they are never mixed with (or
    # resumed against) rows from the earlier independent-operations corpus.
    corpus_label = "synthetic_chained" if args.corpus == "synthetic" else args.corpus

    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    done = set() if args.fresh else resume.completed_cells(
        args.out, EXPERIMENT, args.profile, KEY_COLUMNS)
    written = skipped = 0

    for chain_length, operation_mode in cells(args.deployment, lengths):
        # Read-only, so the corpus is built once and shared by every repetition.
        # Pending repetitions are resolved before paying for that build.
        pending = [repetition for repetition in range(profile["reps"])
                   if resume.cell_key(
                       {"experiment": EXPERIMENT, "deployment": args.deployment,
                        "corpus": corpus_label, "operation_mode": operation_mode,
                        "chain_length": chain_length,
                        "repetition": repetition}, KEY_COLUMNS) not in done]
        skipped += profile["reps"] - len(pending)
        if not pending:
            print(f"[{EXPERIMENT}] {args.deployment} / chain={chain_length} "
                  f"/ {operation_mode}: all reps done, skipping", flush=True)
            continue

        print(f"[{EXPERIMENT}] {args.deployment} / chain={chain_length} "
              f"/ {operation_mode}", flush=True)
        handle = corpus_module.build_corpus(
            args.corpus, primary_uri=primary, records=profile["records"],
            chain_length=chain_length, operation_mode=operation_mode,
            write_concern=WRITE_CONCERN, max_files=profile["real_max_files"],
            domain=domain, chained=True,
        )
        try:
            specs = runner.client_specs(
                profile["clients"], "primary", "split",
                operation_mode, WRITE_CONCERN, primary, [])
            for repetition in pending:
                result = run_workload(specs, handle, read_ratio=1.0,
                                      warmup_s=profile["warmup_s"],
                                      measure_s=profile["measure_s"])
                row = runner.row_from(
                    result, experiment=EXPERIMENT, profile_name=args.profile,
                    profile=profile, deployment=args.deployment,
                    corpus_kind=corpus_label, repetition=repetition,
                    corpus_handle=handle, mongo_version=mongo_version,
                    read_target="primary", read_mode="split",
                    operation_mode=operation_mode, write_concern=WRITE_CONCERN,
                    chain_length=chain_length, mix="read_only")
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
