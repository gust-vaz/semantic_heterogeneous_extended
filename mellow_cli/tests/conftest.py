import os
import uuid
import pytest
from mellow_cli.session import MellowSession

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb://localhost:27017/?directConnection=true")


@pytest.fixture
def make_session():
    created = []

    def _make(mode="preprocess", read_uri=None, read_mode="split", collection="records"):
        db = f"mellowcli_{uuid.uuid4().hex[:12]}"
        s = MellowSession(MONGO_HOST, db, collection=collection,
                          operation_mode=mode, read_uri=read_uri, read_mode=read_mode)
        created.append(s)
        return s

    yield _make
    for s in created:
        try:
            s.drop()
        except Exception:
            pass
