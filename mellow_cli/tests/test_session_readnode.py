def test_status_reports_defaults(make_session):
    s = make_session("preprocess")
    st = s.status()
    assert st["db"] == s.db
    assert st["collection"] == "records"
    assert st["operation_mode"] == "preprocess"
    assert st["read_node"] == "primary"
    assert st["read_mode"] == "split"


def test_set_read_node_to_port_builds_direct_uri(make_session):
    s = make_session("preprocess")
    s.set_read_node("27018", mode="split")
    assert s.status()["read_node"] == "27018"
    assert s._collection.read_uri == "mongodb://localhost:27018/?directConnection=true"

    s.set_read_node("primary")
    assert s.status()["read_node"] == "primary"
    assert s._collection.read_uri is None


def test_status_shows_bare_port_when_constructed_with_read_uri(make_session):
    s = make_session(read_uri="mongodb://localhost:27018/?directConnection=true")
    assert s.status()["read_node"] == "27018"
