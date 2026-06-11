class MellowDBError(Exception):
    """Base class for all MellowDB errors."""


class InvalidOperationArguments(MellowDBError):
    """A semantic operation was registered with missing or malformed arguments."""


class UnsupportedDirectionError(MellowDBError):
    """The operation cannot be applied in the requested direction
    (e.g. evolving a merged record backward, or a pre-split record forward)."""


class VersionChainConflict(MellowDBError):
    """A concurrent registration already modified this position of the
    version chain; the operation must be retried."""
