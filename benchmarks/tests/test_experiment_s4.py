from benchmarks.experiments.s4_crossover_scale import (
    KEY_COLUMNS, READ_RATIO, SHARD_KEY, cells,
)


def test_both_strategies_and_both_mixes_are_swept():
    assert set(cells()) == {
        ("preprocess", "read_heavy"), ("preprocess", "write_heavy"),
        ("rewrite", "read_heavy"), ("rewrite", "write_heavy")}


def test_read_heavy_is_ninety_five_percent_reads():
    assert READ_RATIO["read_heavy"] == 0.95
    assert READ_RATIO["write_heavy"] == 0.05


def test_the_shard_key_is_fixed_because_the_axis_here_is_scale():
    """S1 and S3 are the experiments that vary the shard key. This one varies
    strategy and shard count, so the key stays at the realistic anchor: a data
    field the operations never touch."""
    assert SHARD_KEY == {"role": "data", "kind": "hashed"}


def test_the_key_columns_do_not_mention_the_shard_key():
    assert "shard_key_role" not in KEY_COLUMNS
    assert "shard_key_kind" not in KEY_COLUMNS
    for column in ["experiment", "deployment", "corpus", "operation_mode",
                   "mix", "repetition"]:
        assert column in KEY_COLUMNS


def test_the_sweep_is_the_same_whatever_the_deployment():
    """sh1 gives the intercept and sh4/sh8 the slope, so all three must run the
    identical set of cells or the line is fitted through different things."""
    assert cells() == cells()
    assert len(cells()) == 4
