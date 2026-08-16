"""B4 - Strategy x distribution.

Does distribution change which strategy wins? Replication makes preprocess's
insert-time burst costlier, while read offloading relieves rewrite's query-time
cost - so the single-node winner may flip. Setup phase timings are recorded
because that is exactly where preprocess pays and rewrite does not.
"""

import sys

from benchmarks.harness import corpus as corpus_module
from benchmarks.harness import results, runner, topology
from benchmarks.harness.profiles import get_profile
from benchmarks.harness.workload import run_workload

EXPERIMENT = "b4"
OPERATION_MODES = ["preprocess", "rewrite"]
MIXES = {"read_heavy": 0.95, "write_heavy": 0.05}
READ_TARGETS = ["primary", "secondaries"]
WRITE_CONCERN = "majority"
CHAIN_LENGTH = 5


def cells(deployment, mixes=None):
    """(operation_mode, read_target, mix) triples for this deployment.

    A single node has no secondaries, so it contributes half the cells.
    """
    mix_names = list(mixes or MIXES)
    targets = (["primary"] if topology.node_count(deployment) == 1
               else list(READ_TARGETS))
    return [(mode, target, mix)
            for mode in OPERATION_MODES
            for target in targets
            for mix in mix_names]


def main(argv=None):
    parser = runner.base_parser(EXPERIMENT)
    parser.add_argument("--mix", default=None, choices=sorted(MIXES),
                        help="Run only this mix instead of both")
    args = parser.parse_args(argv)
    profile = get_profile(args.profile, clients=args.clients, records=args.records)

    primary, secondaries = runner.resolve_endpoints(args.deployment)
    mongo_version = topology.server_version(primary)

    selected = [args.mix] if args.mix else None
    out_path = results.result_path(args.out, EXPERIMENT, args.profile)
    rows = []

    for operation_mode, read_target, mix_name in cells(args.deployment, selected):
        for repetition in range(profile["reps"]):
            print(f"[{EXPERIMENT}] {args.deployment} / {operation_mode} / "
                  f"{read_target} / {mix_name} rep={repetition}", flush=True)
            runner.warn_if_target_unavailable(read_target, secondaries)
            handle = corpus_module.build_corpus(
                args.corpus, primary_uri=primary, records=profile["records"],
                chain_length=CHAIN_LENGTH, operation_mode=operation_mode,
                write_concern=WRITE_CONCERN, max_files=profile["real_max_files"],
            )
            try:
                specs = runner.client_specs(
                    profile["clients"], read_target, "split",
                    operation_mode, WRITE_CONCERN, primary, secondaries)
                result = run_workload(specs, handle, read_ratio=MIXES[mix_name],
                                      warmup_s=profile["warmup_s"],
                                      measure_s=profile["measure_s"])
                row = runner.row_from(
                    result, experiment=EXPERIMENT, profile_name=args.profile,
                    profile=profile, deployment=args.deployment,
                    corpus_kind=args.corpus, repetition=repetition,
                    corpus_handle=handle, mongo_version=mongo_version,
                    read_target=read_target, read_mode="split",
                    operation_mode=operation_mode, write_concern=WRITE_CONCERN,
                    chain_length=CHAIN_LENGTH, mix=mix_name)
                runner.warn_if_noisy(row)
                rows.append(row)
            finally:
                corpus_module.drop_corpus(primary, handle)

    results.write_rows(out_path, rows)
    print(f"[{EXPERIMENT}] wrote {len(rows)} rows to {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
