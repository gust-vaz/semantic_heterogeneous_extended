"""
When a newly registered evolution produces records that an already-registered
evolution should have transformed further, the existing evolution must be
reapplied to those records (the cascade in check_if_operation_affected_*).

Regression context: the backward cascade never fired because the version
lookup filtered on a non-existent field ('previous_version_number' instead of
'previous_version'), and the forward/backward gating reused the query
rewriting flags, which run in the opposite direction for grouping/ungrouping.
"""
from datetime import datetime


def _processed_cities(col):
    rows = col.collection.collection_processed.find({}, {'_id': 0, 'city': 1})
    return sorted(r['city'] for r in rows)


def test_translation_backward_cascade(make_collection):
    """A->B@2000 registered first, then B->C@2010: a record carrying C must
    also receive the older A representation for early versions."""
    col = make_collection('preprocess')
    col.insert_one('{"city": "C", "pop": 1}', datetime(2020, 1, 1))

    col.execute_operation(
        'translation', datetime(2000, 1, 1),
        {'fieldName': 'city', 'oldValue': 'A', 'newValue': 'B'}
    )
    col.execute_operation(
        'translation', datetime(2010, 1, 1),
        {'fieldName': 'city', 'oldValue': 'B', 'newValue': 'C'}
    )

    assert _processed_cities(col) == ['A', 'B', 'C']


def test_grouping_forward_cascade(make_collection):
    """[X,Y]->Z@2010 registered first, then W->X@2000: a record carrying W
    must evolve W -> X -> Z across the chain."""
    col = make_collection('preprocess')
    col.insert_one('{"city": "W", "pop": 1}', datetime(1990, 1, 1))

    col.execute_operation(
        'merging', datetime(2010, 1, 1),
        {'fieldName': 'city', 'oldValues': ['X', 'Y'], 'newValue': 'Z'}
    )
    col.execute_operation(
        'translation', datetime(2000, 1, 1),
        {'fieldName': 'city', 'oldValue': 'W', 'newValue': 'X'}
    )

    assert _processed_cities(col) == ['W', 'X', 'Z']


def test_ungrouping_backward_cascade(make_collection):
    """Q->[B,C]@2000 registered first, then C->D@2010: a record carrying D
    must also receive C and Q representations for earlier versions."""
    col = make_collection('preprocess')
    col.insert_one('{"city": "D", "pop": 1}', datetime(2020, 1, 1))

    col.execute_operation(
        'splitting', datetime(2000, 1, 1),
        {'fieldName': 'city', 'oldValue': 'Q', 'newValues': ['B', 'C']}
    )
    col.execute_operation(
        'translation', datetime(2010, 1, 1),
        {'fieldName': 'city', 'oldValue': 'C', 'newValue': 'D'}
    )

    assert _processed_cities(col) == ['C', 'D', 'Q']
