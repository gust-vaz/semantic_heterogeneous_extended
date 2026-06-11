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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
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
