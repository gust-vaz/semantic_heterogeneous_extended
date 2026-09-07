from datetime import datetime

import pytest

from semantic_heterogeneous_database import sharding
from semantic_heterogeneous_database.exceptions import MellowDBError
from semantic_heterogeneous_database.sharding import ShardKey


def test_parse_accepts_mongodb_key_patterns():
    assert ShardKey.parse({'municipio': 1}).kind == 'ranged'
    assert ShardKey.parse({'municipio': 'hashed'}).kind == 'hashed'
    assert ShardKey.parse({'_id': 'hashed'}).field == '_id'


def test_parse_passes_none_through():
    assert ShardKey.parse(None) is None


def test_parse_is_idempotent_on_a_shardkey():
    key = ShardKey('municipio', 'hashed')
    assert ShardKey.parse(key) is key


def test_pattern_round_trips_to_what_shardcollection_wants():
    assert ShardKey.parse({'municipio': 1}).pattern == {'municipio': 1}
    assert ShardKey.parse({'municipio': 'hashed'}).pattern == {'municipio': 'hashed'}
    assert ShardKey.parse({'municipio': -1}).pattern == {'municipio': 1}


def test_equality_is_by_pattern():
    assert ShardKey.parse({'municipio': 1}) == ShardKey('municipio', 'ranged')
    assert ShardKey.parse({'municipio': 1}) != ShardKey('municipio', 'hashed')
    assert ShardKey.parse({'municipio': 1}) != ShardKey('cid', 'ranged')


@pytest.mark.parametrize('bad', [
    {'a': 1, 'b': 1},          # compound is out of scope
    {},                        # empty
    {'municipio': 'text'},     # not a shard key direction
    'municipio',               # not a key pattern
    ['municipio'],
])
def test_parse_rejects_unsupported_specs(bad):
    with pytest.raises(MellowDBError):
        ShardKey.parse(bad)


def test_bad_kind_is_rejected():
    with pytest.raises(MellowDBError):
        ShardKey('municipio', 'zscore')


class FakeAdmin:
    def __init__(self, hello):
        self._hello = hello

    def command(self, name, *args, **kwargs):
        assert name == 'hello'
        return self._hello


class FakeClient:
    def __init__(self, hello):
        self.admin = FakeAdmin(hello)


def test_is_mongos_recognises_a_router():
    assert sharding.is_mongos(FakeClient({'msg': 'isdbgrid', 'ok': 1})) is True


def test_is_mongos_is_false_for_a_replica_set():
    assert sharding.is_mongos(FakeClient({'setName': 'rs0', 'ok': 1})) is False


def test_is_mongos_is_false_for_a_standalone():
    assert sharding.is_mongos(FakeClient({'ok': 1})) is False


def test_describe_returns_none_for_an_unsharded_collection(make_collection):
    col = make_collection('preprocess')
    assert sharding.describe(col.collection.db, 'col') is None


def test_distribution_is_empty_for_an_unsharded_collection(make_collection):
    col = make_collection('preprocess')
    col.insert_one('{"municipio": "Grao Para"}', datetime(1984, 1, 1))
    # collStats reports no `shards` key at all off a cluster; the caller gets {}
    # rather than an exception, so measurement code needs no special case.
    assert sharding.distribution(col.collection.db, 'col_processed') == {}
