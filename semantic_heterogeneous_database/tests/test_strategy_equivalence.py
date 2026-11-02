"""
Preprocess and rewrite must return the same records with the same values, and both
must match the semantics the paper defines, whatever order things were registered in.

The oracle below works out the expected answer in plain Python from the records and
the operations alone, without any of the library's version arithmetic:

* Definition 3.3: an operation evolves a record forward when t < t_h and its value
  is affected. Applying every operation chronologically gives the record's value at
  the latest semantic version, which is what a query returns.
* Definition 3.4 and Equation 8: a query for q also finds r at t >= t_h, and a query
  for r also finds q at t < t_h, each up to the neighbouring hop on that value. An
  exact match on the stored value is always part of the answer.

Grouping cannot be followed forward and ungrouping cannot be followed backward, since
neither can tell which value a record would become (Section 3.2).
"""
import json
import random
from datetime import datetime

import pandas as pd
import pytest

CITIES = ["A", "B", "C", "D", "E"]
COUNTRIES = ["X", "Y", "Z"]
OP_YEARS = [1950, 1960, 1970, 1980]
RECORD_YEARS = [1940, 1950, 1955, 1960, 1965, 1970, 1975, 1980, 1990]
FOREVER = (datetime.min, datetime.max)


class Op:
    def __init__(self, kind, year, field, sources, targets):
        self.kind, self.field = kind, field
        self.date = datetime(year, 1, 1)
        self.sources, self.targets = sources, targets

    @property
    def goes_forward(self):
        return self.kind in ("translation", "splitting")

    @property
    def goes_backward(self):
        return self.kind in ("translation", "merging")

    @property
    def materializes_forward(self):
        return self.kind in ("translation", "merging")

    def register(self, col):
        if self.kind == "translation":
            args = {"oldValue": self.sources[0], "newValue": self.targets[0]}
        elif self.kind == "merging":
            args = {"oldValues": self.sources, "newValue": self.targets[0]}
        else:
            args = {"oldValue": self.sources[0], "newValues": self.targets}
        col.execute_operation(self.kind, self.date, {"fieldName": self.field, **args})


def _chronological(ops):
    """Ties keep registration order: a later registration sits later in the chain."""
    return sorted(ops, key=lambda op: op.date)


def _latest(record, ops):
    values = {"city": record["city"], "country": record["country"]}
    for op in ops:
        if op.materializes_forward and record["t"] < op.date and values[op.field] in op.sources:
            values[op.field] = op.targets[0]
    return values["city"], values["country"]


def _terms(field, value, ops):
    terms = {(value, *FOREVER)}
    ops = [op for op in ops if op.field == field]

    def forward(term, after):
        for i in range(after + 1, len(ops)):
            if ops[i].goes_forward and term in ops[i].sources:
                for target in ops[i].targets:
                    end = next((later.date for later in ops[i + 1:] if target in later.sources),
                               datetime.max)
                    terms.add((target, ops[i].date, end))
                    forward(target, i)

    def backward(term, before):
        for i in range(before - 1, -1, -1):
            if ops[i].goes_backward and term in ops[i].targets:
                for source in ops[i].sources:
                    start = next((earlier.date for earlier in reversed(ops[:i])
                                  if source in earlier.targets), datetime.min)
                    terms.add((source, start, ops[i].date))
                    backward(source, i)

    forward(value, -1)
    backward(value, len(ops))
    return terms


def _expected(records, ops, query):
    ops = _chronological(ops)
    found = {}
    for record in records:
        matches = all(
            any(record[field] == term and start <= record["t"] < end
                for term, start, end in _terms(field, value, ops))
            for field, value in query.items())
        if matches:
            found[record["rid"]] = _latest(record, ops)
    return found


def _returned(col, query):
    docs = list(col.find_many(query))
    rids = [doc["rid"] for doc in docs]
    assert len(rids) == len(set(rids)), f"a record came back twice for {query}: {sorted(rids)}"
    return {doc["rid"]: (doc["city"], doc["country"]) for doc in docs}


def _insert(col, how, records):
    if not records:
        return
    if how == "single":
        for record in records:
            col.insert_one(json.dumps({"rid": record["rid"], "city": record["city"],
                                       "country": record["country"]}), record["t"])
    else:
        col.insert_many_by_dataframe(pd.DataFrame(
            [{"rid": r["rid"], "city": r["city"], "country": r["country"], "at": r["t"]}
             for r in records]), "at")


def _build(col, records, ops, how, order):
    if order == "ops_first":
        steps = [ops, records, [], []]
    elif order == "records_first":
        steps = [[], records, ops, []]
    else:
        cut = len(records) // 2
        half = len(ops) // 2
        steps = [ops[:half], records[:cut], ops[half:], records[cut:]]
    for op in steps[0]:
        op.register(col)
    _insert(col, how, steps[1])
    for op in steps[2]:
        op.register(col)
    _insert(col, how, steps[3])


def _scenario(seed):
    rng = random.Random(seed)
    ops = []
    for _ in range(rng.randint(2, 6)):
        kind = rng.choice(["translation", "translation", "merging", "splitting"])
        field = rng.choice(["city", "city", "city", "country"])
        domain = CITIES if field == "city" else COUNTRIES
        year = rng.choice(OP_YEARS)
        if kind == "translation":
            source, target = rng.sample(domain, 2)
            ops.append(Op(kind, year, field, [source], [target]))
        elif kind == "merging":
            *sources, target = rng.sample(domain, 3)
            ops.append(Op(kind, year, field, sources, [target]))
        else:
            source, *targets = rng.sample(domain, 3)
            ops.append(Op(kind, year, field, [source], targets))
    records = [{"rid": rid, "city": rng.choice(CITIES), "country": rng.choice(COUNTRIES),
                "t": datetime(rng.choice(RECORD_YEARS), 1, 1)} for rid in range(25)]
    return records, ops


def _assert_all_queries_match(make_collection, records, ops, how, order):
    preprocess = make_collection("preprocess")
    rewrite = make_collection("rewrite")
    for col in (preprocess, rewrite):
        _build(col, records, ops, how, order)

    queries = [{"city": city} for city in CITIES] + [{"country": c} for c in COUNTRIES]
    queries.append({"city": "A", "country": "X"})
    for query in queries:
        expected = _expected(records, ops, query)
        assert _returned(preprocess, query) == expected, f"preprocess, {query}"
        assert _returned(rewrite, query) == expected, f"rewrite, {query}"


@pytest.mark.parametrize("seed", range(12))
@pytest.mark.parametrize("order", ["ops_first", "records_first", "mixed"])
@pytest.mark.parametrize("how", ["single", "bulk"])
def test_both_strategies_match_the_semantics(make_collection, seed, order, how):
    records, ops = _scenario(seed)
    _assert_all_queries_match(make_collection, records, ops, how, order)


@pytest.mark.parametrize("seed", range(6))
def test_registration_order_of_different_dates_does_not_change_the_answer(make_collection, seed):
    """Same operations on distinct dates, registered oldest-first and newest-first."""
    rng = random.Random(1000 + seed)
    years = rng.sample(range(1941, 1995), 5)
    ops = []
    for year in years:
        field = rng.choice(["city", "country"])
        domain = CITIES if field == "city" else COUNTRIES
        source, target = rng.sample(domain, 2)
        ops.append(Op("translation", year, field, [source], [target]))
    records = [{"rid": rid, "city": rng.choice(CITIES), "country": rng.choice(COUNTRIES),
                "t": datetime(rng.randint(1940, 1995), 1, 1)} for rid in range(25)]

    oldest_first = make_collection("preprocess")
    newest_first = make_collection("preprocess")
    _build(oldest_first, records, _chronological(ops), "bulk", "records_first")
    _build(newest_first, records, _chronological(ops)[::-1], "bulk", "records_first")

    for city in CITIES:
        assert _returned(oldest_first, {"city": city}) == _returned(newest_first, {"city": city})


def test_the_paper_example_on_record_dates(make_collection):
    """A->B at 1950: a stale A stays A, and a B older than the rename is not A."""
    records = [
        {"rid": 0, "city": "A", "country": "X", "t": datetime(1900, 1, 1)},
        {"rid": 1, "city": "A", "country": "X", "t": datetime(1960, 1, 1)},
        {"rid": 2, "city": "B", "country": "X", "t": datetime(1900, 1, 1)},
        {"rid": 3, "city": "B", "country": "X", "t": datetime(1960, 1, 1)},
    ]
    ops = [Op("translation", 1950, "city", ["A"], ["B"]),
           Op("translation", 2010, "country", ["P"], ["Q"])]
    for mode in ("preprocess", "rewrite"):
        col = make_collection(mode)
        _build(col, records, ops, "single", "records_first")
        assert set(_returned(col, {"city": "A"})) == {0, 1, 3}, mode
        assert set(_returned(col, {"city": "B"})) == {0, 2, 3}, mode
