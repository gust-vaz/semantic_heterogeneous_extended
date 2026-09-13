"""Sharded-cluster support: shard key handling, detection, and placement.

Everything sharding-specific lives here rather than in Collection, which is
already large. The vocabulary is MongoDB's own: a shard key is written as the
key pattern that shardCollection takes, e.g. {'municipio': 'hashed'}.
"""

import warnings

from .exceptions import MellowDBError


class ShardKey:
    """A single-field shard key. Compound keys are deliberately out of scope."""

    __slots__ = ('field', 'kind')

    def __init__(self, field, kind):
        if kind not in ('ranged', 'hashed'):
            raise MellowDBError(
                f"Shard key kind must be 'ranged' or 'hashed', got {kind!r}")
        self.field = field
        self.kind = kind

    @classmethod
    def parse(cls, spec):
        """Accept MongoDB's key pattern: {'municipio': 1} or {'municipio': 'hashed'}."""
        if spec is None or isinstance(spec, cls):
            return spec
        if not isinstance(spec, dict) or len(spec) != 1:
            raise MellowDBError(
                "shard_key must be a single-field key pattern such as "
                "{'municipio': 1} or {'municipio': 'hashed'}, got " + repr(spec))
        (field, value), = spec.items()
        if value == 'hashed':
            return cls(field, 'hashed')
        if value in (1, -1):
            return cls(field, 'ranged')
        raise MellowDBError(
            f"shard_key value must be 1, -1 or 'hashed', got {value!r}")

    @property
    def pattern(self):
        """The dict shardCollection and create_index both want."""
        return {self.field: 'hashed' if self.kind == 'hashed' else 1}

    def __eq__(self, other):
        return isinstance(other, ShardKey) and self.pattern == other.pattern

    def __hash__(self):
        return hash((self.field, self.kind))

    def __repr__(self):
        return f'ShardKey({self.pattern!r})'


def is_mongos(client):
    """True when this client is talking to a query router rather than a mongod."""
    return client.admin.command('hello').get('msg') == 'isdbgrid'


def describe(db, name):
    """The shard key currently in effect for db.name, or None if unsharded.

    Read from the sharding catalog rather than inferred, so a collection sharded
    outside MellowDB is discovered rather than re-sharded.
    """
    entry = db.client['config'].collections.find_one(
        {'_id': f'{db.name}.{name}', 'dropped': {'$ne': True}})
    if entry is None:
        return None
    return ShardKey.parse(dict(entry['key']))


def distribution(db, name):
    """Document count per shard.

    Empty only when the deployment has no shards at all - a standalone mongod or
    a plain replica set. On a cluster an *unsharded* collection still reports one
    entry: the primary shard that owns the whole of it. That is not a quirk to
    paper over, it is the most direct way to see the funnel this work removes.

    Diagnostic, not a gate: MellowDB is schemaless, so a document missing the
    shard key field lands under null and concentrates on one chunk. This makes
    that visible to whoever is measuring instead of failing their write.
    """
    stats = db.command('collStats', name)
    shards = stats.get('shards')
    if not shards:
        return {}
    return {shard: info.get('count', 0) for shard, info in shards.items()}


def ensure(db, name, shard_key):
    """Bring db.name to the requested shard key, idempotently.

    Returns the ShardKey in effect, or None when the deployment is not sharded or
    no key was requested and none exists. Never re-shards: a collection already
    sharded on a different key is an error, not something to silently accept.
    """
    requested = ShardKey.parse(shard_key)

    if not is_mongos(db.client):
        if requested is not None:
            warnings.warn(
                f"shard_key={requested.pattern} ignored: this deployment is "
                f"not a mongos", RuntimeWarning, stacklevel=2)
        return None

    current = describe(db, name)
    if current is not None:
        if requested is not None and requested != current:
            raise MellowDBError(
                f"{db.name}.{name} is already sharded on {current.pattern}; "
                f"refusing to re-shard to {requested.pattern}. Drop the "
                f"collection, or reshard it outside MellowDB.")
        return current

    if requested is None:
        return None

    db.client.admin.command('enableSharding', db.name)

    ## An empty collection gets its shard key index created by shardCollection.
    ## A populated one does not: it fails with InvalidOptions (72) unless the
    ## index already exists, so build it first.
    if (name in db.list_collection_names()
            and db[name].estimated_document_count() > 0):
        db[name].create_index(list(requested.pattern.items()))

    db.client.admin.command(
        'shardCollection', f'{db.name}.{name}', key=requested.pattern)
    return requested
