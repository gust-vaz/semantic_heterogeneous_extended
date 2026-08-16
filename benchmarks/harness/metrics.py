"""Pure functions turning latency samples into reportable numbers."""

import math

ERROR_RATE_THRESHOLD = 0.01


def percentile(samples, p):
    """Nearest-rank percentile: index = ceil(p/100 * n) - 1, clamped."""
    if not samples:
        return 0.0
    ordered = sorted(samples)
    index = max(0, math.ceil(p / 100 * len(ordered)) - 1)
    return ordered[index]


def error_rate(ops, errors):
    """Failed attempts as a fraction of all attempts."""
    attempts = ops + errors
    if attempts == 0:
        return 0.0
    return errors / attempts


def exceeds_error_threshold(ops, errors):
    """True when the error rate is above 1% - the row should be flagged."""
    return error_rate(ops, errors) > ERROR_RATE_THRESHOLD


def summarize(latencies_ms, elapsed_s, errors=0):
    """Latency samples plus a window length -> the metric columns of a result row."""
    ops = len(latencies_ms)
    return {
        "ops": ops,
        "errors": errors,
        "error_rate": round(error_rate(ops, errors), 6),
        "throughput_ops_s": round(ops / elapsed_s, 3) if elapsed_s > 0 else 0.0,
        "mean_ms": round(sum(latencies_ms) / ops, 3) if ops else 0.0,
        "p50_ms": round(percentile(latencies_ms, 50), 3),
        "p95_ms": round(percentile(latencies_ms, 95), 3),
        "p99_ms": round(percentile(latencies_ms, 99), 3),
    }
