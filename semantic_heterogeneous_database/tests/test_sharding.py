import os
import uuid as _uuid
import warnings
from datetime import datetime

import pytest
from pymongo import MongoClient

from semantic_heterogeneous_database import BasicCollection, sharding
from semantic_heterogeneous_database.exceptions import MellowDBError
from semantic_heterogeneous_database.sharding import ShardKey

MONGO_HOST = os.environ.get(
    "MONGO_HOST", "mongodb://localhost:27017/?directConnection=true")

needs_sharding = pytest.mark.skipif(
    not os.environ.get("MELLOW_SHARDED"),
    reason="needs a mongos; set MELLOW_SHARDED=1 and point MONGO_HOST at it")


@pytest.fixture
def scratch_db():
    client = MongoClient(MONGO_HOST)
    db = client[f"mellowshard_{_uuid.uuid4().hex[:12]}"]
    yield db
    client.drop_database(db.name)


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
    # shard_key=None explicitly: omitting it would inherit MELLOW_SHARD_KEY and
    # shard the collection under a sharded run, which is not what this asserts.
    col = make_collection('preprocess', shard_key=None)
    assert sharding.describe(col.collection.db, 'col') is None


def test_distribution_is_empty_off_a_cluster(make_collection):
    col = make_collection('preprocess')
    if sharding.is_mongos(col.collection.client):
        pytest.skip("this assertion is about deployments with no shards")
    col.insert_one('{"municipio": "Grao Para"}', datetime(1984, 1, 1))
    # collStats reports no `shards` key at all off a cluster; the caller gets {}
    # rather than an exception, so measurement code needs no special case.
    assert sharding.distribution(col.collection.db, 'col_processed') == {}


@needs_sharding
def test_distribution_names_the_owning_shard_of_an_unsharded_collection(scratch_db):
    """On a cluster, an unsharded collection is not invisible to collStats: it
    reports the one shard holding all of it. That is how the primary-shard funnel
    becomes measurable rather than inferred.

    Built directly rather than through make_collection so no environment-level
    shard key can be applied behind this assertion's back.
    """
    scratch_db.col.insert_many([{'municipio': 'Grao Para'} for _ in range(5)])
    placement = sharding.distribution(scratch_db, 'col')
    assert len(placement) == 1
    assert sum(placement.values()) == 5


def test_ensure_is_a_noop_and_warns_when_not_sharded(scratch_db):
    if sharding.is_mongos(scratch_db.client):
        pytest.skip("this assertion is about non-sharded deployments")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert sharding.ensure(scratch_db, 'col', {'municipio': 1}) is None
    assert any('not a mongos' in str(w.message) for w in caught)


def test_ensure_is_silent_when_no_key_is_requested(scratch_db):
    if sharding.is_mongos(scratch_db.client):
        pytest.skip("this assertion is about non-sharded deployments")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert sharding.ensure(scratch_db, 'col', None) is None
    assert caught == []


@needs_sharding
def test_ensure_shards_an_empty_collection(scratch_db):
    scratch_db.create_collection('col')
    key = sharding.ensure(scratch_db, 'col', {'municipio': 'hashed'})
    assert key == ShardKey('municipio', 'hashed')
    assert sharding.describe(scratch_db, 'col') == key


@needs_sharding
def test_ensure_shards_a_populated_collection(scratch_db):
    scratch_db.col.insert_many([{'municipio': f'm{i}'} for i in range(100)])
    key = sharding.ensure(scratch_db, 'col', {'municipio': 1})
    assert sharding.describe(scratch_db, 'col') == key


@needs_sharding
def test_ensure_is_idempotent(scratch_db):
    scratch_db.create_collection('col')
    first = sharding.ensure(scratch_db, 'col', {'municipio': 1})
    second = sharding.ensure(scratch_db, 'col', {'municipio': 1})
    assert first == second


@needs_sharding
def test_ensure_refuses_to_change_an_existing_shard_key(scratch_db):
    scratch_db.create_collection('col')
    sharding.ensure(scratch_db, 'col', {'municipio': 1})
    with pytest.raises(MellowDBError) as exc:
        sharding.ensure(scratch_db, 'col', {'cid': 'hashed'})
    assert 'already sharded' in str(exc.value)


@needs_sharding
def test_ensure_discovers_an_existing_key_when_none_is_requested(scratch_db):
    scratch_db.create_collection('col')
    sharding.ensure(scratch_db, 'col', {'municipio': 'hashed'})
    assert sharding.ensure(scratch_db, 'col', None) == ShardKey('municipio', 'hashed')


@needs_sharding
def test_ensure_leaves_a_collection_unsharded_when_no_key_is_requested(scratch_db):
    scratch_db.create_collection('col')
    assert sharding.ensure(scratch_db, 'col', None) is None
    assert sharding.describe(scratch_db, 'col') is None


def test_collection_exposes_sharding_state(make_collection):
    col = make_collection('preprocess')
    assert isinstance(col.collection._is_sharded, bool)
    assert col.collection._shard_key is None or isinstance(
        col.collection._shard_key, ShardKey)


def test_bad_shard_key_is_rejected_at_construction():
    with pytest.raises(MellowDBError):
        BasicCollection(f"mellowshard_{_uuid.uuid4().hex[:12]}", "col",
                        MONGO_HOST, 'preprocess', shard_key={'a': 1, 'b': 1})


@needs_sharding
def test_preprocess_shards_both_raw_and_processed(scratch_db):
    BasicCollection(scratch_db.name, "col", MONGO_HOST, 'preprocess',
                    shard_key={'municipio': 'hashed'})
    assert sharding.describe(scratch_db, 'col') == ShardKey('municipio', 'hashed')
    assert sharding.describe(scratch_db, 'col_processed') == ShardKey('municipio', 'hashed')


@needs_sharding
def test_rewrite_shards_only_the_raw_collection(scratch_db):
    BasicCollection(scratch_db.name, "col", MONGO_HOST, 'rewrite',
                    shard_key={'municipio': 1})
    assert sharding.describe(scratch_db, 'col') == ShardKey('municipio', 'ranged')
    assert sharding.describe(scratch_db, 'col_processed') is None


@needs_sharding
def test_metadata_collections_are_never_sharded(scratch_db):
    BasicCollection(scratch_db.name, "col", MONGO_HOST, 'preprocess',
                    shard_key={'municipio': 'hashed'})
    assert sharding.describe(scratch_db, 'col_versions') is None
    assert sharding.describe(scratch_db, 'col_columns') is None
