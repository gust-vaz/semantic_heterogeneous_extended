from datetime import datetime


def _write_csv(path, text):
    path.write_text(text, encoding="utf-8")


def test_load_csv_folder_then_query(make_session, tmp_path):
    folder = tmp_path / "src"
    folder.mkdir()
    _write_csv(folder / "y2001.csv",
               "city,RefDate,ocorrencias\nPiracicaba,2001-12-31,5\n")
    s = make_session("preprocess")
    s.load_csv(str(folder), date_field="RefDate")
    assert s.count({"city": "Piracicaba"}) == 1


def test_apply_operations_csv_translates_query(make_session, tmp_path):
    folder = tmp_path / "src"
    folder.mkdir()
    _write_csv(folder / "y2001.csv",
               "cid,RefDate,ocorrencias\nOldName,2001-12-31,5\n")
    ops = tmp_path / "ops.csv"
    _write_csv(ops, "from;to;type;valid_from;field\nOldName;NewName;translation;2002-01-01;cid\n")

    s = make_session("preprocess")
    s.load_csv(str(folder), date_field="RefDate")
    s.apply_operations_csv(str(ops))
    # the old record is now reachable under the current (new) term
    assert s.count({"cid": "NewName"}) == 1
