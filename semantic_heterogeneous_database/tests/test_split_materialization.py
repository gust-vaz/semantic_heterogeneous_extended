"""The materialization step must not create a temporary collection and must not
re-read its own writes.

Both properties are what let this run on a sharded cluster: a temporary
collection is unsharded and therefore lands entirely on the primary shard, and
$merge reading the collection it writes to can otherwise see its own inserts.
"""
import json
import os
import re
from datetime import datetime

import pytest

UUID_NAMED = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-')

needs_sharding = pytest.mark.skipif(
    not os.environ.get("MELLOW_SHARDED"),
    reason="needs a mongos; set MELLOW_SHARDED=1 and point MONGO_HOST at it")


@needs_sharding
def test_records_missing_the_shard_key_field_are_still_split(make_collection):
    """MellowDB is schemaless, so a record may lack the field _processed is
    sharded on. $merge refuses such a record ("'on' field cannot be missing",
    Location51132) and fails only after other shards have written their copies,
    leaving orphan copies over originals that never get bounded. Those records
    must be split anyway.

    Three keyless records against a batch size of two also cross a batch
    boundary, so losing the tail of the client-side path fails this test too.
    """
    col = make_collection('preprocess', shard_key={'municipio': 'hashed'})
    col.collection._CLIENT_SPLIT_BATCH = 2
    for i in range(5):
        col.insert_one(json.dumps({'city': 'Leningrad', 'municipio': f'm{i}'}),
                       datetime(1984, 1, 1))
    for i in range(3):
        col.insert_one(json.dumps({'city': 'Leningrad', 'i': i}), datetime(1984, 1, 1))
    col.insert_one(json.dumps({'city': 'Moscow'}), datetime(1984, 1, 1))

    col.execute_operation('translation', datetime(1996, 1, 1), {
        'fieldName': 'city', 'oldValue': 'Leningrad', 'newValue': 'Saint Petersburg'})

    processed = col.collection.collection_processed
    inf = float('inf')
    assert processed.count_documents(
        {'city': 'Saint Petersburg', 'municipio': {'$exists': True}}) == 5
    assert processed.count_documents(
        {'city': 'Saint Petersburg', 'municipio': {'$exists': False}}) == 3
    assert processed.count_documents({'city': 'Leningrad'}) == 8
    assert processed.count_documents({'city': 'Leningrad', '_max_version_number': inf}) == 0
    assert processed.count_documents({'city': 'Moscow', '_max_version_number': inf}) == 1
    assert processed.count_documents({}) == 17


def test_split_leaves_no_temporary_collection(make_collection):
    col = make_collection('preprocess')
    col.insert_one(json.dumps({'municipio': 'Grao Para', 'ocorrencias': 1}),
                   datetime(1984, 1, 1))

    col.execute_operation('translation', datetime(1996, 1, 1), {
        'fieldName': 'municipio', 'oldValue': 'Grao Para', 'newValue': 'Grao-Para'})

    names = col.collection.db.list_collection_names()
    assert [n for n in names if UUID_NAMED.match(n)] == []


def test_split_does_not_reprocess_its_own_output(make_collection):
    """A record split once must yield exactly two rows, not a runaway chain."""
    col = make_collection('preprocess')
    for i in range(50):
        col.insert_one(json.dumps({'municipio': 'Grao Para', 'ocorrencias': i}),
                       datetime(1984, 1, 1))

    col.execute_operation('translation', datetime(1996, 1, 1), {
        'fieldName': 'municipio', 'oldValue': 'Grao Para', 'newValue': 'Grao-Para'})

    processed = col.collection.collection_processed
    assert processed.count_documents({'municipio': 'Grao Para'}) == 50
    assert processed.count_documents({'municipio': 'Grao-Para'}) == 50
    assert processed.count_documents({}) == 100


def test_a_value_that_does_not_change_is_not_split(make_collection):
    """A merge whose target is also one of its sources must split only the rows
    that actually change.

    Without the $ne guard the pipeline copies a record whose value already equals
    the evolved value, producing two rows that carry the same value over adjacent
    version ranges. That is redundant rather than wrong, but it is also exactly
    the shape that would let $merge see its own inserts, so the guard removes it.
    """
    col = make_collection('preprocess')
    col.insert_one(json.dumps({'municipio': 'Grao Para', 'ocorrencias': 1}),
                   datetime(1984, 1, 1))
    col.insert_one(json.dumps({'municipio': 'Gram Para', 'ocorrencias': 2}),
                   datetime(1984, 1, 1))

    col.execute_operation('merging', datetime(1996, 1, 1), {
        'fieldName': 'municipio',
        'oldValues': ['Grao Para', 'Gram Para'],
        'newValue': 'Grao Para'})

    processed = col.collection.collection_processed
    # 'Gram Para' genuinely becomes 'Grao Para', so that record splits in two.
    assert processed.count_documents({'ocorrencias': 2}) == 2
    # 'Grao Para' was already the target value: nothing changed, nothing splits.
    assert processed.count_documents({'ocorrencias': 1}) == 1
