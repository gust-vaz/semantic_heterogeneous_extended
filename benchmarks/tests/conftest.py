import os
import pytest

MONGO_HOST = os.environ.get(
    "MONGO_HOST", "mongodb://localhost:27017/?directConnection=true"
)


@pytest.fixture
def primary_uri():
    return MONGO_HOST


@pytest.fixture
def cleanup_corpus():
    """Drop every corpus a test builds, even when the test fails."""
    from benchmarks.harness.corpus import drop_corpus
    built = []

    def _track(uri, handle):
        built.append((uri, handle))
        return handle

    yield _track
    for uri, handle in built:
        try:
            drop_corpus(uri, handle)
        except Exception:
            pass
