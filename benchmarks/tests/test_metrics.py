from benchmarks.harness.metrics import (
    percentile, error_rate, exceeds_error_threshold, summarize,
)

TEN = [float(i) for i in range(1, 11)]  # 1.0 .. 10.0


def test_percentile_uses_nearest_rank():
    # nearest-rank: index = ceil(p/100 * n) - 1
    assert percentile(TEN, 50) == 5.0
    assert percentile(TEN, 95) == 10.0
    assert percentile(TEN, 99) == 10.0
    assert percentile(TEN, 100) == 10.0


def test_percentile_of_empty_is_zero():
    assert percentile([], 95) == 0.0


def test_percentile_does_not_require_sorted_input():
    unsorted = [10.0, 1.0, 5.0, 3.0]
    assert percentile(unsorted, 50) == percentile(sorted(unsorted), 50)
    assert percentile(unsorted, 50) == 3.0  # nearest-rank of 4 samples -> index 1


def test_percentile_of_single_sample():
    assert percentile([7.0], 99) == 7.0


def test_error_rate_counts_errors_as_attempts():
    assert error_rate(99, 1) == 0.01
    assert error_rate(0, 0) == 0.0


def test_exceeds_threshold_only_above_one_percent():
    assert exceeds_error_threshold(99, 1) is False   # exactly 1%
    assert exceeds_error_threshold(90, 10) is True   # 10%
    assert exceeds_error_threshold(100, 0) is False


def test_summarize_computes_throughput_over_the_window():
    result = summarize(TEN, elapsed_s=2.0, errors=0)
    assert result["ops"] == 10
    assert result["throughput_ops_s"] == 5.0
    assert result["mean_ms"] == 5.5
    assert result["p50_ms"] == 5.0
    assert result["p95_ms"] == 10.0
    assert result["errors"] == 0
    assert result["error_rate"] == 0.0


def test_summarize_handles_no_operations():
    result = summarize([], elapsed_s=5.0, errors=3)
    assert result["ops"] == 0
    assert result["throughput_ops_s"] == 0.0
    assert result["mean_ms"] == 0.0
    assert result["errors"] == 3
    assert result["error_rate"] == 1.0


def test_summarize_handles_zero_elapsed():
    assert summarize(TEN, elapsed_s=0.0)["throughput_ops_s"] == 0.0
