"""
Bulk inserts must materialize the same processed rows as single inserts when
a record is affected by a chain of semantic evolutions.

Regression context: __insert_many_by_version removed consumed rows from the
working set by `_original_id`, which also discarded sibling rows of the same
original record that had NOT matched the operation. With a chain A->B->C the
oldest row (value A) was silently dropped, so historical queries missed the
record entirely.
"""
import pandas as pd
from datetime import datetime


def _processed_cities(col):
    rows = col.collection.collection_processed.find({}, {'_id': 0, 'city': 1})
    return sorted(r['city'] for r in rows)


def test_bulk_insert_keeps_all_rows_under_chained_translations(make_collection):
    col = make_collection('preprocess')
    col.execute_operation(
        'translation', datetime(2000, 1, 1),
        {'fieldName': 'city', 'oldValue': 'A', 'newValue': 'B'}
    )
    col.execute_operation(
        'translation', datetime(2010, 1, 1),
        {'fieldName': 'city', 'oldValue': 'B', 'newValue': 'C'}
    )

    df = pd.DataFrame([{'city': 'A', 'pop': 1, 'valid_from_date': datetime(1990, 1, 1)}])
    col.insert_many_by_dataframe(df, 'valid_from_date')

    assert _processed_cities(col) == ['A', 'B', 'C']


def test_bulk_insert_matches_single_insert_under_chained_translations(make_collection):
    """The bulk path must produce exactly what insert_one produces."""
    bulk = make_collection('preprocess')
    single = make_collection('preprocess')

    for col in (bulk, single):
        col.execute_operation(
            'translation', datetime(2000, 1, 1),
            {'fieldName': 'city', 'oldValue': 'A', 'newValue': 'B'}
        )
        col.execute_operation(
            'translation', datetime(2010, 1, 1),
            {'fieldName': 'city', 'oldValue': 'B', 'newValue': 'C'}
        )

    df = pd.DataFrame([{'city': 'A', 'pop': 1, 'valid_from_date': datetime(1990, 1, 1)}])
    bulk.insert_many_by_dataframe(df, 'valid_from_date')
    single.insert_one('{"city": "A", "pop": 1}', datetime(1990, 1, 1))

    assert _processed_cities(bulk) == _processed_cities(single)
