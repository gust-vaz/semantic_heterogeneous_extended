"""
Bulk inserts must tolerate documents that lack a field with registered
semantic evolutions (MongoDB collections are schemaless).

Regression context: check_if_many_affected joined the inserted DataFrame on
every field present in the version chain, raising KeyError when the
DataFrame had no such column.
"""
import pandas as pd
from datetime import datetime


def test_insert_many_without_evolved_field_does_not_crash(make_collection, count):
    col = make_collection('preprocess')

    col.execute_operation(
        'translation', datetime(2000, 1, 1),
        {'fieldName': 'city', 'oldValue': 'A', 'newValue': 'B'}
    )
    col.execute_operation(
        'merging', datetime(2005, 1, 1),
        {'fieldName': 'region', 'oldValues': ['R1', 'R2'], 'newValue': 'R3'}
    )
    col.execute_operation(
        'splitting', datetime(2010, 1, 1),
        {'fieldName': 'state', 'oldValue': 'S1', 'newValues': ['S2', 'S3']}
    )

    # No 'city', 'region' or 'state' column at all
    df = pd.DataFrame([
        {'pop': 10, 'valid_from_date': datetime(1990, 1, 1)},
        {'pop': 20, 'valid_from_date': datetime(2015, 1, 1)},
    ])
    col.insert_many_by_dataframe(df, 'valid_from_date')

    assert count(col, {'pop': 10}) == 1
    assert count(col, {'pop': 20}) == 1
