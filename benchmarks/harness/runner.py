"""Shared plumbing every experiment module uses: CLI, client fan-out, row building."""

import argparse
import os
import sys

from benchmarks.harness import metrics, results, topology
from benchmarks.harness.profiles import DEFAULT_PROFILE, PROFILES
from benchmarks.harness.workload import ClientSpec

READ_TARGETS = ["primary", "secondaries", "secondaries_single_source"]


def base_parser(experiment):
    parser = argparse.ArgumentParser(prog=f"benchmarks.experiments.{experiment}")
    parser.add_argument("--deployment", required=True, choices=sorted(topology.DEPLOYMENTS))
    parser.add_argument("--profile", default=DEFAULT_PROFILE, choices=sorted(PROFILES))
    parser.add_argument("--corpus", default="synthetic", choices=["synthetic", "real"])
    parser.add_argument("--out", default="results",
                        help="Output directory for result CSVs")
    parser.add_argument("--clients", type=int, default=None,
                        help="Override the profile's client count")
    parser.add_argument("--records", type=int, default=None,
                        help="Override the profile's synthetic record count")
    return parser


def resolve_endpoints(deployment):
    """Return (write_uri, secondary_read_uris) for this deployment.

    The write URI names the whole replica set so writes survive an election;
    only the read URIs are pinned to individual nodes. Inside the runner
    container the Compose service hostnames resolve, so those come from
    topology. BENCH_PRIMARY_URI / BENCH_SECONDARY_URIS let a host-side run
    (tests, manual probing) point at ports instead, since those hostnames do
    not resolve outside the Compose network.
    """
    primary_override = os.environ.get("BENCH_PRIMARY_URI")
    if not primary_override:
        return topology.write_uri(deployment), topology.secondary_uris(deployment)

    raw = os.environ.get("BENCH_SECONDARY_URIS", "")
    secondaries = [uri.strip() for uri in raw.split(",") if uri.strip()]
    return primary_override, secondaries


def warn_if_target_unavailable(read_target, secondaries):
    """Guard against a row labelled 'secondaries' whose reads went to the primary."""
    if read_target.startswith("secondaries") and not secondaries:
        print(
            f"WARNING: read_target='{read_target}' requested but no secondaries "
            "are available; reads fall back to the primary and this row's "
            "read_target label will not reflect where reads actually went",
            file=sys.stderr,
        )


def read_targets_for(deployment):
    """A single node has no secondaries, so only the primary target applies."""
    if topology.node_count(deployment) == 1:
        return ["primary"]
    return list(READ_TARGETS)


def client_specs(clients, read_target, read_mode, operation_mode,
                 write_concern, primary, secondaries):
    """Build one ClientSpec per worker, fanning reads across nodes round-robin.

    Writes always go to `primary` - MongoDB accepts them nowhere else.
    """
    specs = []
    for index in range(clients):
        if read_target == "primary" or not secondaries:
            read_uri, mode = None, read_mode
        elif read_target == "secondaries":
            read_uri, mode = secondaries[index % len(secondaries)], "split"
        elif read_target == "secondaries_single_source":
            read_uri, mode = secondaries[index % len(secondaries)], "single_source"
        else:
            raise ValueError(f"Unknown read target '{read_target}'")
        specs.append(ClientSpec(mongo_uri=primary, operation_mode=operation_mode,
                                write_concern=write_concern,
                                read_uri=read_uri, read_mode=mode))
    return specs


def row_from(result, *, experiment, profile_name, profile, deployment,
             corpus_kind, repetition, corpus_handle, mongo_version, **extra):
    """Assemble one full-schema result row from a WorkloadResult."""
    summary = metrics.summarize(result.latencies_ms, result.elapsed_s, result.errors)
    values = {
        "experiment": experiment,
        "profile": profile_name,
        "deployment": deployment,
        "nodes": topology.node_count(deployment),
        "corpus": corpus_kind,
        "repetition": repetition,
        "clients": profile["clients"],
        "warmup_s": profile["warmup_s"],
        "measure_s": profile["measure_s"],
        "records": corpus_handle.record_count,
        "setup_insert_s": round(corpus_handle.setup_insert_s, 3),
        "setup_operations_s": round(corpus_handle.setup_operations_s, 3),
        "read_p95_ms": (round(metrics.percentile(result.read_latencies_ms, 95), 3)
                        if result.read_latencies_ms else ""),
        "write_p95_ms": (round(metrics.percentile(result.write_latencies_ms, 95), 3)
                         if result.write_latencies_ms else ""),
    }
    values.update(summary)
    values.update(results.run_metadata(mongo_version=mongo_version))
    values.update(extra)
    return results.make_row(**values)


def warn_if_noisy(row):
    """Print a warning when a cell's error rate makes its numbers untrustworthy."""
    if metrics.exceeds_error_threshold(row["ops"], row["errors"]):
        rate = metrics.error_rate(row["ops"], row["errors"])
        print(
            f"WARNING: {row.get('deployment')}/{row.get('read_target')} "
            f"error rate {rate:.1%} exceeds "
            f"{metrics.ERROR_RATE_THRESHOLD:.0%} - treat this row with suspicion",
            file=sys.stderr,
        )
