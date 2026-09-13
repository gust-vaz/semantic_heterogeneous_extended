"""
Tests for the synthetic benchmark database generator.

Regression context: simulations.py seeds the global `random` module, and the
generator used to draw its database/collection names from that seeded stream.
Every benchmark process therefore reused the same database name, so a crashed
run left a stale database that the next run silently attached to, inheriting
a foreign version chain (KeyError on fields that no longer exist).
"""
import os
import random
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'benchmarks')))
from database_generator import DatabaseGenerator

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb://localhost:27017/?directConnection=true")


def _generate_tiny(generator):
    generator.generate(
        number_of_records=1,
        number_of_versions=1,
        number_of_fields=8,
        number_of_values_in_domain=4,
        number_of_evolution_fields=1,
        operation_mode='preprocess',
    )


def test_database_name_unique_even_with_same_seed():
    """Two runs with an identical random seed must not share a database."""
    random.seed(42)
    d1 = DatabaseGenerator(host=MONGO_HOST)
    _generate_tiny(d1)
    name1 = d1.database_name
    d1.destroy()

    random.seed(42)
    d2 = DatabaseGenerator(host=MONGO_HOST)
    _generate_tiny(d2)
    name2 = d2.database_name
    d2.destroy()

    assert name1 != name2, (
        "Seeded runs produced the same database name; a crashed run would "
        "poison every subsequent benchmark run"
    )


def test_generator_forwards_read_config_to_collection():
    """read_uri/read_mode passed to the generator reach the underlying collection."""
    random.seed(7)
    d = DatabaseGenerator(host=MONGO_HOST, read_uri=MONGO_HOST, read_mode='single_source')
    _generate_tiny(d)
    try:
        assert d.collection.read_uri == MONGO_HOST
        assert d.collection.read_mode == 'single_source'
    finally:
        d.destroy()


def _generate(generator, fields, evolution_fields):
    generator.generate(
        number_of_records=3,
        number_of_versions=1,
        number_of_fields=fields,
        number_of_values_in_domain=20,
        number_of_evolution_fields=evolution_fields,
        operation_mode='rewrite',
    )


def test_field_names_can_be_chosen_before_generating():
    """A shard key has to be named before the corpus exists, so field names cannot
    come out of the random stream."""
    random.seed(3)
    d = DatabaseGenerator(host=MONGO_HOST)
    _generate(d, fields=6, evolution_fields=2)
    try:
        assert d.evolution_field_names() == ['evo0', 'evo1']
        assert d.field_names() == ['evo0', 'evo1', 'f0', 'f1', 'f2', 'f3']
    finally:
        d.destroy()


def test_every_requested_evolution_field_is_a_distinct_field():
    """Drawing evolution fields with random.choice in a loop could pick one field
    twice, so asking for four could silently evolve fewer."""
    for seed in range(10):
        random.seed(seed)
        d = DatabaseGenerator(host=MONGO_HOST)
        _generate(d, fields=8, evolution_fields=4)
        try:
            names = [name for name, _ in d.evolution_fields]
            assert len(set(names)) == 4, (seed, names)
        finally:
            d.destroy()


def test_more_evolution_fields_than_fields_is_rejected():
    random.seed(5)
    d = DatabaseGenerator(host=MONGO_HOST)
    try:
        with pytest.raises(ValueError):
            _generate(d, fields=2, evolution_fields=3)
    finally:
        if hasattr(d, 'collection'):
            d.destroy()


def test_generator_forwards_shard_key_to_collection():
    """shard_key passed to the generator reaches the underlying collection."""
    random.seed(7)
    d = DatabaseGenerator(host=MONGO_HOST, shard_key={'evo0': 'hashed'})
    _generate_tiny(d)
    try:
        assert d.collection.shard_key == {'evo0': 'hashed'}
    finally:
        d.destroy()
