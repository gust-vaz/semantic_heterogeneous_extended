"""What sharding is supposed to buy, asserted against a real cluster.

Every test here needs a mongos and is skipped otherwise, so the suite still runs
against a standalone mongod. The absence of the $out funnel itself is asserted in
test_split_materialization.py, from the commands MellowDB sends, because the old
implementation's temporary collection left nothing behind to inspect afterwards.
"""
import os
from datetime import datetime

import pandas as pd
import pytest

from semantic_heterogeneous_database import sharding
from semantic_heterogeneous_database.sharding import ShardKey

pytestmark = pytest.mark.skipif(
    not os.environ.get("MELLOW_SHARDED"),
    reason="needs a mongos; set MELLOW_SHARDED=1 and point MONGO_HOST at it")

MUNICIPIOS = ['Ararangua', 'Blumenau', 'Chapeco', 'Criciuma', 'Grao Para',
              'Joinville', 'Lages', 'Palhoca', 'Tubarao', 'Videira']
TRANSLATION = {'fieldName': 'municipio', 'oldValue': 'Grao Para', 'newValue': 'Zortea'}


def _seed(col):
    """40 records per municipio: 400 in all, 40 of them 'Grao Para'."""
    col.insert_many_by_dataframe(pd.DataFrame([
        {'municipio': municipio, 'ocorrencias': i, 'RefDate': datetime(1984, 1, 1)}
        for municipio in MUNICIPIOS for i in range(40)]), 'RefDate')


def test_evolved_copies_land_on_the_shard_their_new_value_routes_to(make_collection):
    """A semantic shard key relocates data: renaming a municipio writes its copies
    wherever the new name hashes to - all of them, and nowhere else."""
    col = make_collection('preprocess', shard_key={'municipio': 'hashed'})
    _seed(col)
    db = col.collection.db
    before = sharding.distribution(db, 'col_processed')
    assert len([n for n in before.values() if n]) > 1, \
        f"corpus sits on one shard, so nothing is proven: {before}"

    col.execute_operation('translation', datetime(1996, 1, 1), TRANSLATION)

    after = sharding.distribution(db, 'col_processed')
    gained = {shard: after.get(shard, 0) - before.get(shard, 0) for shard in after}
    assert sorted(gained.values()) == [0] * (len(gained) - 1) + [40], gained
    owner = max(gained, key=gained.get)

    plan = col.collection.collection_processed.find({'municipio': 'Zortea'}).explain()
    assert [s['shardName'] for s in plan['queryPlanner']['winningPlan']['shards']] == [owner]


def test_merge_into_the_sharded_collection_needs_no_unique_index_beyond_id(make_collection):
    """Pins behaviour that a literal reading of the MongoDB docs contradicts.

    The docs say $merge into a sharded collection defaults `on` to the shard key plus
    _id and needs a unique index over exactly those fields. On 8.0.12 the default path
    performs no such check. If an upgrade changes that, this breaks before any data
    does; the fix is an explicit `on` plus a unique {field: 1, _id: 1} index, which is
    creatable even under a hashed shard key.
    """
    col = make_collection('preprocess', shard_key={'municipio': 'hashed'})
    _seed(col)
    processed = col.collection.collection_processed
    assert sharding.describe(col.collection.db, 'col_processed') == ShardKey('municipio', 'hashed')
    # listIndexes carries no `unique` flag on the implicit _id_ index (measured), so
    # any flagged index here is one this test's premise does not allow.
    assert [idx['name'] for idx in processed.list_indexes() if idx.get('unique')] == []

    col.execute_operation('translation', datetime(1996, 1, 1), TRANSLATION)

    assert processed.count_documents({'municipio': 'Grao Para'}) == 40
    assert processed.count_documents({'municipio': 'Zortea'}) == 40


@pytest.mark.parametrize('shard_key', [
    {'_id': 'hashed'},
    {'_id': 1},
    {'municipio': 1},
    {'municipio': 'hashed'},
])
def test_every_shard_key_shape_reprocesses_correctly(make_collection, shard_key):
    col = make_collection('preprocess', shard_key=shard_key)
    _seed(col)

    col.execute_operation('translation', datetime(1996, 1, 1), TRANSLATION)

    processed = col.collection.collection_processed
    assert processed.count_documents({'municipio': 'Grao Para'}) == 40
    assert processed.count_documents({'municipio': 'Zortea'}) == 40
    assert processed.count_documents({}) == 440
