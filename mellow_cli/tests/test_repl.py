from datetime import datetime
from mellow_cli.repl import Repl


def test_status_and_query_via_handle(make_session):
    s = make_session("preprocess")
    s.insert('{"city": "Bauru"}', datetime(2001, 1, 1))
    repl = Repl(s)

    assert "preprocess" in repl.handle("status")
    assert "1" in repl.handle("count {'city': 'Bauru'}")
    assert "Bauru" in repl.handle("query {'city': 'Bauru'}")


def test_read_node_switch_via_handle(make_session):
    s = make_session("preprocess")
    repl = Repl(s)
    repl.handle("read-node 27018 single_source")
    assert s.status()["read_node"] == "27018"
    assert s.status()["read_mode"] == "single_source"


def test_exit_returns_none(make_session):
    repl = Repl(make_session("preprocess"))
    assert repl.handle("exit") is None


def test_destroy_requires_yes(make_session):
    repl = Repl(make_session("preprocess"))
    out = repl.handle("destroy")
    assert "--yes" in out


def test_load_then_operation_via_handle(make_session, tmp_path):
    folder = tmp_path / "src"
    folder.mkdir()
    (folder / "y.csv").write_text("cid,RefDate,ocorrencias\nOld,2001-12-31,5\n", encoding="utf-8")
    s = make_session("preprocess")
    repl = Repl(s)
    repl.handle(f"load {folder} RefDate")
    assert s.count({"cid": "Old"}) == 1
    repl.handle("operation translation 2002-01-01 "
                "{'fieldName':'cid','oldValue':'Old','newValue':'New'}")
    assert s.count({"cid": "New"}) == 1


def test_operations_csv_via_handle(make_session, tmp_path):
    folder = tmp_path / "src"
    folder.mkdir()
    (folder / "y.csv").write_text("cid,RefDate,ocorrencias\nX,2001-12-31,5\n", encoding="utf-8")
    ops = tmp_path / "ops.csv"
    ops.write_text("from;to;type;valid_from;field\nX;Y;translation;2002-01-01;cid\n", encoding="utf-8")
    s = make_session("preprocess")
    repl = Repl(s)
    repl.handle(f"load {folder} RefDate")
    repl.handle(f"operations {ops}")
    assert s.count({"cid": "Y"}) == 1
