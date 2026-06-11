"""
MellowDB raises its own exception hierarchy instead of BaseException /
argparse.ArgumentError / bare RuntimeError, so callers can write a sane
`except MellowDBError:` boundary.
"""
import os
import pytest
from datetime import datetime

from semantic_heterogeneous_database import BasicCollection
from semantic_heterogeneous_database.exceptions import (
    MellowDBError,
    InvalidOperationArguments,
    UnsupportedDirectionError,
    VersionChainConflict,
)

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb://localhost:27017/?directConnection=true")


def test_hierarchy():
    for exc in (InvalidOperationArguments, UnsupportedDirectionError, VersionChainConflict):
        assert issubclass(exc, MellowDBError)
    assert issubclass(MellowDBError, Exception)


def test_invalid_operation_mode_raises_mellowdb_error():
    with pytest.raises(MellowDBError):
        BasicCollection("any_db", "col", MONGO_HOST, "bogus_mode")


def test_missing_operation_argument_raises_invalid_arguments(make_collection):
    col = make_collection('preprocess')
    with pytest.raises(InvalidOperationArguments):
        col.execute_operation('translation', datetime(2000, 1, 1), {'fieldName': 'city'})
    with pytest.raises(InvalidOperationArguments):
        col.execute_operation('merging', datetime(2000, 1, 1),
                              {'fieldName': 'city', 'oldValues': 'not-a-list', 'newValue': 'X'})
    with pytest.raises(InvalidOperationArguments):
        col.execute_operation('splitting', datetime(2000, 1, 1), {'oldValue': 'Q'})
