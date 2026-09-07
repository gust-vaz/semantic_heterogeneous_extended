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
