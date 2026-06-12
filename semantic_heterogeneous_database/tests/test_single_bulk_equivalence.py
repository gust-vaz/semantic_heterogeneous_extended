"""
insert_one and insert_many_by_dataframe must materialize identical processed
rows: the bulk path is an optimization of the single path, and the single
path is the ground truth for the distributed consistency experiments.

Regression context: the two paths used different matching predicates —
inclusive vs strict version bounds, and the record's own date vs the date of
the record's version — so the same record could be materialized differently
depending on which insert API was used.
"""
import json
import random
import pandas as pd
import pytest
from datetime import datetime


OPERATIONS = [
    ('translation', datetime(2000, 1, 1), {'fieldName': 'city', 'oldValue': 'A', 'newValue': 'B'}),
    ('translation', datetime(2010, 1, 1), {'fieldName': 'city', 'oldValue': 'B', 'newValue': 'C'}),
    ('merging', datetime(2005, 1, 1), {'fieldName': 'city', 'oldValues': ['M1', 'M2'], 'newValue': 'M'}),
    ('splitting', datetime(2015, 1, 1), {'fieldName': 'city', 'oldValue': 'S', 'newValues': ['S1', 'S2']}),
]


def _materialize(col, records):
    """Return the multiset of (city, _min, _max) rows the insert produced,
    with version numbers normalized to chain positions (the raw numbers are
    random floats and differ between collections for the same chain)."""
    numbers = sorted(v['version_number'] for v in col.collection.collection_versions.find())
    rank = {n: i for i, n in enumerate(numbers)}
    rank[float('-inf')] = float('-inf')
    rank[float('inf')] = float('inf')

    rows = col.collection.collection_processed.find(
        {}, {'_id': 0, 'city': 1, '_min_version_number': 1, '_max_version_number': 1})
    return sorted((r['city'], rank[r['_min_version_number']], rank[r['_max_version_number']]) for r in rows)


def _compare(make_collection, records):
    single = make_collection('preprocess')
    bulk = make_collection('preprocess')
    for col in (single, bulk):
        for op_type, valid_from, args in OPERATIONS:
            col.execute_operation(op_type, valid_from, args)

    for r in records:
        single.insert_one(json.dumps({'city': r['city'], 'pop': r['pop']}), r['valid_from_date'])
    bulk.insert_many_by_dataframe(pd.DataFrame(records), 'valid_from_date')

    assert _materialize(single, records) == _materialize(bulk, records)


def test_new_value_dated_before_operation(make_collection):
    """A record carrying the NEW value but valid before the operation date
    must be treated the same by both paths (this was the known divergence)."""
    _compare(make_collection, [{'city': 'B', 'pop': 1, 'valid_from_date': datetime(1990, 1, 1)}])


def test_old_value_after_operation(make_collection):
    _compare(make_collection, [{'city': 'A', 'pop': 1, 'valid_from_date': datetime(2012, 1, 1)}])


def test_chained_record_before_everything(make_collection):
    _compare(make_collection, [{'city': 'A', 'pop': 1, 'valid_from_date': datetime(1990, 1, 1)}])


def test_randomized_equivalence(make_collection):
    """Random records over the operations' domain, fixed seed."""
    rng = random.Random(7)
    domain = ['A', 'B', 'C', 'M', 'M1', 'M2', 'S', 'S1', 'S2', 'X']
    records = [{
        'city': rng.choice(domain),
        'pop': i,
        'valid_from_date': datetime(rng.randint(1985, 2024), rng.randint(1, 12), rng.randint(1, 28)),
    } for i in range(40)]
    _compare(make_collection, records)
