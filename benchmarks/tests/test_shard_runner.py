import pytest

from benchmarks.harness import shard_runner
from benchmarks.harness.profiles import DEFAULT_PROFILE


def test_only_sharded_deployments_are_offered():
    # The B series' axes - read_target, read_mode, write_concern - mean nothing
    # on single-node shards, so the two series share no CLI.
    parser = shard_runner.base_parser("s1")
    assert parser.parse_args(["--deployment", "sh4"]).deployment == "sh4"
    with pytest.raises(SystemExit):
        parser.parse_args(["--deployment", "rs3"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--deployment", "single"])


def test_sh1_is_offered_because_it_is_the_scale_intercept():
    assert shard_runner.base_parser("s1").parse_args(
        ["--deployment", "sh1"]).deployment == "sh1"


def test_resuming_is_the_default():
    assert shard_runner.base_parser("s1").parse_args(
        ["--deployment", "sh4"]).fresh is False


def test_the_profile_default_follows_the_configured_one():
    assert shard_runner.base_parser("s1").parse_args(
        ["--deployment", "sh4"]).profile == DEFAULT_PROFILE


def test_chunk_size_is_an_explicit_parameter_not_mongodbs_default():
    # MongoDB's built-in 128 MB makes the balancer inert at any corpus size this
    # campaign can afford, so the value is declared and recorded in every row.
    args = shard_runner.base_parser("s1").parse_args(["--deployment", "sh4"])
    assert args.chunk_size_mb == shard_runner.DEFAULT_CHUNK_SIZE_MB == 1
    assert shard_runner.base_parser("s1").parse_args(
        ["--deployment", "sh4", "--chunk-size-mb", "64"]).chunk_size_mb == 64


def test_shard_row_fills_provenance_and_the_deployment_shard_count():
    row = shard_runner.shard_row(experiment="s1", deployment="sh4",
                                 profile="smoke", mongo_version="8.0.12")
    assert row["shards"] == 4
    assert row["mongo_version"] == "8.0.12"
    assert row["host_cpus"] >= 1
    assert row["started_at"]


def test_shard_row_rejects_a_replica_set_column():
    with pytest.raises(ValueError) as exc:
        shard_runner.shard_row(experiment="s1", deployment="sh4",
                               read_target="primary")
    assert "read_target" in str(exc.value)


def test_summarize_leaves_a_missing_latency_family_empty_not_zero():
    """A read-only cell has no write latencies. Writing 0.0 would be read as a
    very fast write rather than as no write at all."""
    from benchmarks.harness.workload import WorkloadResult

    result = WorkloadResult(latencies_ms=[1.0, 2.0], read_latencies_ms=[1.0, 2.0],
                            write_latencies_ms=[], errors=0, elapsed_s=2.0)
    summary = shard_runner.summarize(result)
    assert summary["write_p95_ms"] == ""
    assert summary["read_p95_ms"] > 0
    assert summary["ops"] == 2


def test_already_done_is_empty_when_fresh_is_asked_for(tmp_path):
    import pandas as pd

    from benchmarks.harness import results

    path = results.result_path(str(tmp_path), "s1", "smoke", today="20260920")
    results.write_rows(path, [results.make_shard_row(
        experiment="s1", profile="smoke", deployment="sh4", repetition=0)],
        columns=results.SHARD_COLUMNS)
    assert pd.read_csv(path).shape[0] == 1

    keys = ["experiment", "deployment", "repetition"]
    args = shard_runner.base_parser("s1").parse_args(
        ["--deployment", "sh4", "--profile", "smoke", "--out", str(tmp_path)])
    assert shard_runner.already_done(args, "s1", keys) == {("s1", "sh4", "0")}

    fresh = shard_runner.base_parser("s1").parse_args(
        ["--deployment", "sh4", "--profile", "smoke", "--out", str(tmp_path),
         "--fresh"])
    assert shard_runner.already_done(fresh, "s1", keys) == set()


def test_emit_writes_one_row_with_the_shard_schema(tmp_path):
    import pandas as pd

    from benchmarks.harness import results

    path = str(tmp_path / "s1.csv")
    shard_runner.emit(path, shard_runner.shard_row(
        experiment="s1", deployment="sh8", profile="smoke"))
    frame = pd.read_csv(path)
    assert list(frame.columns) == results.SHARD_COLUMNS
    assert frame["shards"].tolist() == [8]


def test_the_router_uri_comes_from_topology_by_default():
    assert shard_runner.router_uri("sh4") == "mongodb://mongos:27017"


def test_a_host_side_run_can_point_at_a_published_port(monkeypatch):
    """`mongos` does not resolve outside the Compose network, so a test or a
    probe run from the host needs somewhere else to point."""
    monkeypatch.setenv("BENCH_ROUTER_URI", "mongodb://localhost:27017")
    assert shard_runner.router_uri("sh4") == "mongodb://localhost:27017"
