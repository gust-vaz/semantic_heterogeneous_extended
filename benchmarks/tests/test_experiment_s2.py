import pytest

from benchmarks.experiments.s2_rebalance import (
    BALANCER, EXPERIMENT, KEY_COLUMNS, RIDES_ALONG_WITH_S1,
)


def test_s2_declares_the_balancer_on():
    assert BALANCER is True


def test_s2_is_the_only_experiment_that_turns_the_balancer_on():
    from benchmarks.experiments.s1_operation_cost import BALANCER as S1_BALANCER

    assert S1_BALANCER is False


def test_s2_shares_s1_key_columns_so_the_pair_lines_up():
    """The two halves of what a semantic shard key costs - applying the
    operation, then restoring balance - must join row for row."""
    from benchmarks.experiments.s1_operation_cost import KEY_COLUMNS as S1_KEYS

    assert set(KEY_COLUMNS) - {"experiment"} == set(S1_KEYS) - {"experiment"}


def test_s2_rides_along_on_s1s_corpus():
    """Building its own would double the most expensive part of the campaign to
    measure a second thing about the same cluster state."""
    assert RIDES_ALONG_WITH_S1 is True
    assert EXPERIMENT == "s2"


def test_arrived_sums_only_what_landed():
    from benchmarks.harness.sharding_metrics import arrived

    # one shard gave 300 away, two received it: 300 moved, not 600
    assert arrived({"s1": 1000, "s2": 0, "s3": 0},
                   {"s1": 700, "s2": 200, "s3": 100}) == 300


def test_arrived_is_zero_when_nothing_moved():
    from benchmarks.harness.sharding_metrics import arrived

    assert arrived({"s1": 10, "s2": 10}, {"s1": 10, "s2": 10}) == 0
