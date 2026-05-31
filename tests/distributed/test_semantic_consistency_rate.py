"""
Semantic consistency rate measurement across different (write_concern, read_concern) pairs.

This is an experimental evaluation file, not a CI gate. It outputs a CSV with
columns: write_concern, read_concern_versions, precision, recall, f1_score.

Run:
    uv run pytest tests/distributed/test_semantic_consistency_rate.py -m slow -v \\
        --output=results_consistency.csv
"""
import csv
import uuid
from datetime import datetime

import pytest
from pymongo import ReadPreference
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from semantic_heterogeneous_database import BasicCollection
from tests.distributed.conftest import MONGO_RS_URI


NUM_QUERY_ROUNDS = 10


def _build_ground_truth(rs_uri: str):
    """
    Build a small dataset with one translation, record correct result sets.
    Returns (col, queries, ground_truth_sets, db_name).
    """
    db_name = f"mellow_consistency_{uuid.uuid4().hex[:8]}"
    col = BasicCollection(db_name, "col", rs_uri, "rewrite", "majority")

    for i in range(10):
        col.insert_one(f'{{"city": "OldCity", "idx": {i}}}', datetime(1990, 1, 1))
    for i in range(10):
        col.insert_one(f'{{"city": "NewCity", "idx": {i + 10}}}', datetime(2010, 1, 1))

    col.execute_operation(
        "translation",
        datetime(2000, 1, 1),
        {"fieldName": "city", "oldValue": "OldCity", "newValue": "NewCity"}
    )

    queries = [{"city": "NewCity"}] * NUM_QUERY_ROUNDS
    ground_truth_sets = []
    for q in queries:
        results = list(col.find_many(q))
        ground_truth_sets.append(frozenset(str(r['_id']) for r in results))

    return col, queries, ground_truth_sets, db_name


def _measure_rate(rs_uri, db_name, queries, ground_truth_sets, write_concern, use_weak_versions):
    """Run queries with given consistency settings and compute precision/recall/F1."""
    col = BasicCollection(db_name, "col", rs_uri, "rewrite", write_concern)

    original_prop = None
    if use_weak_versions:
        original_prop = type(col.collection)._versions_r.fget
        from pymongo.read_concern import ReadConcern as RC
        from pymongo import ReadPreference as RP

        def weak_versions_r(self):
            return self.collection_versions.with_options(
                read_preference=RP.SECONDARY_PREFERRED,
                read_concern=RC('local')
            )
        type(col.collection)._versions_r = property(weak_versions_r)

    precisions, recalls = [], []
    for q, gt in zip(queries, ground_truth_sets):
        results = list(col.find_many(q))
        result_ids = frozenset(str(r['_id']) for r in results)
        precision = len(result_ids & gt) / len(result_ids) if result_ids else 0.0
        recall = len(result_ids & gt) / len(gt) if gt else 1.0
        precisions.append(precision)
        recalls.append(recall)

    if use_weak_versions and original_prop is not None:
        type(col.collection)._versions_r = property(original_prop)

    avg_p = sum(precisions) / len(precisions)
    avg_r = sum(recalls) / len(recalls)
    f1 = (2 * avg_p * avg_r / (avg_p + avg_r)) if (avg_p + avg_r) > 0 else 0.0

    return {
        'write_concern': write_concern,
        'read_concern_versions': 'local' if use_weak_versions else 'majority',
        'precision': round(avg_p, 4),
        'recall': round(avg_r, 4),
        'f1_score': round(f1, 4),
    }


@pytest.mark.slow
def test_semantic_consistency_rate(rs_uri, request):
    """
    Measure semantic consistency rate for 4 (write_concern, read_concern) combinations.
    With directConnection=true to the primary, all reads will go to the same node,
    so 'local' read concern behaves identically to 'majority' (no replication lag).
    We assert that majority-concern reads always give 100% F1.
    """
    col, queries, ground_truth_sets, db_name = _build_ground_truth(rs_uri)

    configs = [
        ("majority", False),
        ("1",        False),
        ("majority", True),
        ("1",        True),
    ]

    results = []
    for wc, weak in configs:
        row = _measure_rate(rs_uri, db_name, queries, ground_truth_sets, wc, weak)
        results.append(row)
        print(f"\n{row}")

    # Majority version reads must always yield 100% F1
    for row in results:
        if row['read_concern_versions'] == 'majority':
            assert row['f1_score'] == 1.0, (
                f"Expected 100% F1 with majority version reads, got {row['f1_score']} "
                f"(write_concern={row['write_concern']})"
            )

    # Write CSV if --output provided
    output = request.config.getoption("--output", default=None)
    if output:
        with open(output, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"\nResults written to {output}")

    col.collection.client.drop_database(db_name)
