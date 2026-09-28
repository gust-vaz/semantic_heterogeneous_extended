"""Shared plumbing for the S series: CLI, row building, resumption.

Deliberately not benchmarks.harness.runner. That module is built around
read_target, read_mode and write_concern, and all three are meaningless on
single-node shards - so the two series share the neutral machinery (profiles,
metrics, workload, corpus, topology) and nothing else.
"""

import argparse
import os

from benchmarks.harness import metrics, resume, results, topology
from benchmarks.harness.profiles import DEFAULT_PROFILE, PROFILES

#: 1 MB, not MongoDB's built-in 128 MB. The balancer moves data only when a
#: collection is unbalanced by size, so at 128 MB it never runs on a corpus this
#: campaign can afford: a measured run of four chunks holding 956 KB produced
#: zero migrations. Declared as a parameter and recorded in every row, because
#: it is an experimental condition rather than a production default.
DEFAULT_CHUNK_SIZE_MB = 1


def base_parser(experiment):
    parser = argparse.ArgumentParser(prog=f"benchmarks.experiments.{experiment}")
    parser.add_argument("--deployment", required=True,
                        choices=sorted(name for name in topology.DEPLOYMENTS
                                       if topology.is_sharded(name)))
    parser.add_argument("--profile", default=DEFAULT_PROFILE, choices=sorted(PROFILES))
    parser.add_argument("--corpus", default="synthetic", choices=["synthetic", "real"])
    parser.add_argument("--out", default="results",
                        help="Output directory for result CSVs")
    parser.add_argument("--records", type=int, default=None,
                        help="Override the profile's synthetic record count")
    parser.add_argument("--clients", type=int, default=None,
                        help="Override the profile's client count")
    parser.add_argument("--chunk-size-mb", type=int, default=DEFAULT_CHUNK_SIZE_MB,
                        dest="chunk_size_mb",
                        help="Cluster-wide chunk size; the balancer is inert above it")
    parser.add_argument("--fresh", action="store_true",
                        help="Ignore rows already written and run every cell again")
    return parser


def router_uri(deployment):
    """Where to reach the cluster's router.

    Inside the runner container the Compose service hostname resolves, so this
    is topology's URI. BENCH_ROUTER_URI lets a host-side run - a test, or a
    probe larger than a profile allows - point at a published port instead,
    since `mongos` does not resolve outside the Compose network. The B series
    carries the same escape hatch as BENCH_PRIMARY_URI.
    """
    return os.environ.get("BENCH_ROUTER_URI") or topology.write_uri(deployment)


def shard_row(**values):
    """One S-series row, with provenance and the deployment's shard count filled in."""
    values.setdefault("shards", topology.shard_count(values["deployment"]))
    mongo_version = values.pop("mongo_version", "")
    values.update(results.run_metadata(mongo_version=mongo_version))
    return results.make_shard_row(**values)


def summarize(result):
    """Workload samples -> the steady-state metric columns of an S-series row.

    A latency family with no samples stays empty rather than 0.0: a read-only
    cell has no writes, and 0.0 would read as an extremely fast one.
    """
    values = metrics.summarize(result.latencies_ms, result.elapsed_s, result.errors)
    values["read_p95_ms"] = (round(metrics.percentile(result.read_latencies_ms, 95), 3)
                             if result.read_latencies_ms else "")
    values["write_p95_ms"] = (round(metrics.percentile(result.write_latencies_ms, 95), 3)
                              if result.write_latencies_ms else "")
    return values


def already_done(args, experiment, key_columns):
    """Cells this experiment has already written, or nothing under --fresh."""
    if args.fresh:
        return set()
    return resume.completed_cells(args.out, experiment, args.profile, key_columns)


def emit(out_path, row):
    """Write one row as it is measured, so an interrupted campaign keeps it."""
    results.write_rows(out_path, [row], columns=results.SHARD_COLUMNS)
