"""Sharded-cluster support: shard key handling, detection, and placement.

Everything sharding-specific lives here rather than in Collection, which is
already large. The vocabulary is MongoDB's own: a shard key is written as the
key pattern that shardCollection takes, e.g. {'municipio': 'hashed'}.
"""

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
    """Document count per shard, or {} when the collection is not sharded.

    Diagnostic, not a gate: MellowDB is schemaless, so a document missing the
    shard key field lands under null and concentrates on one chunk. This makes
    that visible to whoever is measuring instead of failing their write.
    """
    stats = db.command('collStats', name)
    shards = stats.get('shards')
    if not shards:
        return {}
    return {shard: info.get('count', 0) for shard, info in shards.items()}
