from datetime import datetime


def test_insert_then_query_returns_the_record(make_session):
    s = make_session("preprocess")
    s.insert('{"city": "Piracicaba", "pop": 42}', datetime(2001, 1, 1))
    rows = s.query({"city": "Piracicaba"})
    assert len(rows) == 1
    assert rows[0]["city"] == "Piracicaba"
    assert s.count({"city": "Piracicaba"}) == 1
