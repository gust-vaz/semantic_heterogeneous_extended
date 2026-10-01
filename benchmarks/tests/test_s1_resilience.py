"""One cell that cannot be measured must not abort the whole campaign.

The pre-flight guard raises RuntimeError for a cell whose corpus did not end up
where the cell declared it would. On the first real multi-VM run that killed the
entire s1 experiment mid-way; a campaign should skip the bad cell and go on.
"""
from benchmarks.experiments import s1_operation_cost as s1


class _FakeClient:
    def __init__(self, *a, **k):
        pass


def test_a_guard_failure_skips_the_cell_instead_of_aborting(monkeypatch, tmp_path, capsys):
    bad = {"role": "data", "kind": "ranged", "skew": 0.0, "cardinality": 20,
           "negative_control": False}
    good = {"role": "evolved", "kind": "hashed", "skew": 0.0, "cardinality": 20,
            "negative_control": False}

    monkeypatch.setattr(s1, "MongoClient", _FakeClient)
    monkeypatch.setattr(s1.topology, "server_version", lambda uri: "8.0.12")
    monkeypatch.setattr(s1.shard_runner, "already_done", lambda *a, **k: set())
    monkeypatch.setattr(s1, "cells",
                        lambda dep, anchor_only=False: [] if anchor_only else [bad, good])
    monkeypatch.setattr(s1.results, "result_path",
                        lambda *a, **k: str(tmp_path / "s1.csv"))

    calls = {"n": 0}

    def fake_measure(client, args, profile, cell, repetition, mongo_version):
        calls["n"] += 1
        if cell is bad:
            raise RuntimeError("cell sh4/data-ranged: all documents sit on one shard")
        return ({"experiment": "s1"}, _Handle())

    emitted = []
    monkeypatch.setattr(s1, "measure_cell", fake_measure)
    monkeypatch.setattr(s1.shard_runner, "emit", lambda path, row: emitted.append(row))
    monkeypatch.setattr(s1.corpus_module, "drop_corpus", lambda uri, handle: None)

    rc = s1.main(["--deployment", "sh4", "--profile", "smoke"])

    assert rc == 0                      # did not crash on the bad cell
    assert calls["n"] == 2              # it went on to the good cell
    assert len(emitted) == 1           # only the good cell produced a row
    assert "skipped (guard)" in capsys.readouterr().out


class _Handle:
    database_name = "db"
    collection_name = "c"
