import pytest
from benchmarks.harness.runner import (
    base_parser, read_targets_for, client_specs, row_from, warn_if_noisy,
    resolve_endpoints, warn_if_target_unavailable,
)
from benchmarks.harness.workload import WorkloadResult
from benchmarks.harness.corpus import CorpusHandle

PRIMARY = "mongodb://mongo-primary:27017/?directConnection=true"
SECONDARIES = [
    "mongodb://mongo-secondary-1:27018/?directConnection=true",
    "mongodb://mongo-secondary-2:27019/?directConnection=true",
]


def test_base_parser_defaults_to_the_small_profile():
    args = base_parser("b1").parse_args(["--deployment", "rs3"])
    assert args.profile == "small"
    assert args.corpus == "synthetic"
    assert args.out


def test_base_parser_requires_a_deployment():
    with pytest.raises(SystemExit):
        base_parser("b1").parse_args([])


def test_single_deployment_has_only_the_primary_target():
    assert read_targets_for("single") == ["primary"]


def test_replica_sets_add_the_secondary_targets():
    assert read_targets_for("rs3") == ["primary", "secondaries", "secondaries_single_source"]


def test_primary_target_leaves_read_uri_unset():
    specs = client_specs(3, "primary", "split", "preprocess", "majority",
                         PRIMARY, SECONDARIES)
    assert len(specs) == 3
    assert all(spec.read_uri is None for spec in specs)
    assert all(spec.mongo_uri == PRIMARY for spec in specs)


def test_secondary_target_assigns_nodes_round_robin():
    specs = client_specs(4, "secondaries", "split", "preprocess", "majority",
                         PRIMARY, SECONDARIES)
    assert [spec.read_uri for spec in specs] == [
        SECONDARIES[0], SECONDARIES[1], SECONDARIES[0], SECONDARIES[1],
    ]
    assert all(spec.read_mode == "split" for spec in specs)


def test_single_source_target_sets_that_read_mode():
    specs = client_specs(2, "secondaries_single_source", "split",
                         "preprocess", "majority", PRIMARY, SECONDARIES)
    assert all(spec.read_mode == "single_source" for spec in specs)
    assert all(spec.read_uri is not None for spec in specs)


def test_writes_always_target_the_primary():
    specs = client_specs(2, "secondaries", "split", "preprocess",
                         "majority", PRIMARY, SECONDARIES)
    assert all(spec.mongo_uri == PRIMARY for spec in specs)


def test_row_from_populates_schema_and_metrics():
    result = WorkloadResult(latencies_ms=[1.0, 2.0, 3.0, 4.0],
                            read_latencies_ms=[1.0, 2.0, 3.0, 4.0],
                            errors=0, elapsed_s=2.0)
    handle = CorpusHandle("db", "col", [{"a": 1}], 1.5, 0.5, 100)
    row = row_from(result, experiment="b1", profile_name="smoke",
                   profile={"clients": 2, "warmup_s": 1, "measure_s": 2},
                   deployment="rs3", corpus_kind="synthetic", repetition=0,
                   corpus_handle=handle, mongo_version="8.0.12",
                   read_target="secondaries", read_mode="split",
                   operation_mode="preprocess", write_concern="majority")

    assert row["experiment"] == "b1"
    assert row["nodes"] == 3
    assert row["ops"] == 4
    assert row["throughput_ops_s"] == 2.0
    assert row["records"] == 100
    assert row["setup_insert_s"] == 1.5
    assert row["read_p95_ms"] == 4.0
    assert row["write_p95_ms"] == ""
    assert row["host_cpus"] >= 1


def test_warn_if_noisy_flags_a_high_error_rate(capsys):
    warn_if_noisy({"ops": 90, "errors": 10, "deployment": "rs3", "read_target": "primary"})
    assert "error rate" in capsys.readouterr().err.lower()


def test_warn_if_noisy_stays_quiet_when_clean(capsys):
    warn_if_noisy({"ops": 100, "errors": 0, "deployment": "rs3", "read_target": "primary"})
    assert capsys.readouterr().err == ""


def test_resolve_endpoints_honours_both_env_overrides(monkeypatch):
    monkeypatch.setenv("BENCH_PRIMARY_URI", "mongodb://localhost:27017/?directConnection=true")
    monkeypatch.setenv("BENCH_SECONDARY_URIS",
                       "mongodb://localhost:27018/?directConnection=true,"
                       "mongodb://localhost:27019/?directConnection=true")
    primary, secondaries = resolve_endpoints("rs3")
    assert primary.endswith("27017/?directConnection=true")
    assert len(secondaries) == 2
    assert secondaries[1].endswith("27019/?directConnection=true")


def test_resolve_endpoints_tolerates_no_secondary_override(monkeypatch):
    monkeypatch.setenv("BENCH_PRIMARY_URI", "mongodb://localhost:27017/?directConnection=true")
    monkeypatch.delenv("BENCH_SECONDARY_URIS", raising=False)
    _, secondaries = resolve_endpoints("rs3")
    assert secondaries == []


def test_warn_when_secondaries_requested_but_absent(capsys):
    warn_if_target_unavailable("secondaries", [])
    err = capsys.readouterr().err
    assert "secondaries" in err
    assert "fall back to the primary" in err


def test_no_warning_when_secondaries_are_available(capsys):
    warn_if_target_unavailable("secondaries", ["mongodb://s1:27018/?directConnection=true"])
    assert capsys.readouterr().err == ""


def test_no_warning_for_the_primary_target(capsys):
    warn_if_target_unavailable("primary", [])
    assert capsys.readouterr().err == ""
