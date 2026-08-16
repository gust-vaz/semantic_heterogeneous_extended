import pytest
from benchmarks.harness.corpus import build_synthetic
from benchmarks.harness.workload import ClientSpec, WorkloadResult, run_workload


@pytest.fixture
def corpus(primary_uri, cleanup_corpus):
    return cleanup_corpus(primary_uri, build_synthetic(
        primary_uri, records=50, chain_length=0, operation_mode="preprocess"))


def _specs(primary_uri, count):
    return [ClientSpec(mongo_uri=primary_uri, operation_mode="preprocess",
                       write_concern="1", read_uri=None, read_mode="split")
            for _ in range(count)]


def test_digit_string_write_concern_is_coerced_to_an_int():
    # A string "1" is a write-concern MODE NAME to MongoDB and fails every
    # write with UnknownReplWriteConcern; it must become the integer 1.
    spec = ClientSpec(mongo_uri="mongodb://x:1", operation_mode="preprocess",
                      write_concern="1")
    assert spec.write_concern == 1


def test_majority_write_concern_is_left_alone():
    spec = ClientSpec(mongo_uri="mongodb://x:1", operation_mode="preprocess",
                      write_concern="majority")
    assert spec.write_concern == "majority"


def test_read_only_workload_records_only_reads(primary_uri, corpus):
    result = run_workload(_specs(primary_uri, 2), corpus, read_ratio=1.0,
                          warmup_s=1, measure_s=2)
    assert isinstance(result, WorkloadResult)
    assert len(result.read_latencies_ms) > 0
    assert result.write_latencies_ms == []
    assert len(result.latencies_ms) == len(result.read_latencies_ms)
    assert result.elapsed_s == 2


def test_write_only_workload_records_only_writes(primary_uri, corpus):
    result = run_workload(_specs(primary_uri, 2), corpus, read_ratio=0.0,
                          warmup_s=1, measure_s=2)
    assert len(result.write_latencies_ms) > 0
    assert result.read_latencies_ms == []


def test_mixed_workload_records_both_kinds(primary_uri, corpus):
    result = run_workload(_specs(primary_uri, 2), corpus, read_ratio=0.5,
                          warmup_s=1, measure_s=3)
    assert len(result.read_latencies_ms) > 0
    assert len(result.write_latencies_ms) > 0


def test_warmup_samples_are_discarded(primary_uri, corpus):
    long_warmup = run_workload(_specs(primary_uri, 1), corpus, read_ratio=1.0,
                               warmup_s=2, measure_s=1)
    short_warmup = run_workload(_specs(primary_uri, 1), corpus, read_ratio=1.0,
                                warmup_s=0, measure_s=1)
    # Both measured 1s, so counts are comparable; the long warmup must not
    # have accumulated its warmup traffic into the result.
    assert len(long_warmup.latencies_ms) < len(short_warmup.latencies_ms) * 3


def test_more_clients_do_more_work(primary_uri, corpus):
    one = run_workload(_specs(primary_uri, 1), corpus, read_ratio=1.0,
                       warmup_s=1, measure_s=2)
    four = run_workload(_specs(primary_uri, 4), corpus, read_ratio=1.0,
                        warmup_s=1, measure_s=2)
    assert len(four.latencies_ms) > len(one.latencies_ms)


def test_latencies_are_positive_milliseconds(primary_uri, corpus):
    result = run_workload(_specs(primary_uri, 1), corpus, read_ratio=1.0,
                          warmup_s=0, measure_s=2)
    assert all(value > 0 for value in result.latencies_ms)


@pytest.mark.slow
def test_broken_client_spec_is_counted_as_errors_not_raised(primary_uri, corpus):
    # NOTE: this test can take ~30s - pymongo's default server selection
    # timeout - because the unreachable node is only detected on first use.
    broken = [ClientSpec(mongo_uri=primary_uri, operation_mode="preprocess",
                         write_concern="1",
                         read_uri="mongodb://nonexistent-host:27017/?directConnection=true",
                         read_mode="single_source")]
    result = run_workload(broken, corpus, read_ratio=1.0, warmup_s=0, measure_s=2)
    assert result.errors > 0
