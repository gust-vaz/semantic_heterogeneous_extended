"""Corpus construction: populate a collection and hand back a workload's inputs.

Two implementations behind one shape - synthetic (default, runs anywhere) and
real DATASUS. Experiments never know which one they were given.
"""

import json
import random
import time
from dataclasses import dataclass, field

import pandas as pd
from pymongo import MongoClient

from benchmarks.database_generator import DatabaseGenerator

QUERY_SET_SIZE = 50


@dataclass
class CorpusHandle:
    database_name: str
    collection_name: str
    query_set: list
    setup_insert_s: float
    setup_operations_s: float
    record_count: int
    make_record: object = field(repr=False, default=None)


def drop_corpus(primary_uri, handle):
    MongoClient(primary_uri).drop_database(handle.database_name)


def build_synthetic(primary_uri, records, chain_length, operation_mode,
                    write_concern="majority", seed=42, fields=8, domain=20,
                    evolution_fields=2):
    """Fabricate a corpus with DatabaseGenerator and apply `chain_length` operations."""
    random.seed(seed)
    generator = DatabaseGenerator(host=primary_uri, write_concern=write_concern)
    generator.generate(
        number_of_records=records,
        number_of_versions=1,
        number_of_fields=fields,
        number_of_values_in_domain=domain,
        number_of_evolution_fields=evolution_fields,
        operation_mode=operation_mode,
    )

    start = time.time()
    generator.collection.insert_many_by_dataframe(
        pd.DataFrame(generator.records), 'valid_from_date')
    setup_insert_s = time.time() - start

    start = time.time()
    for _ in range(chain_length):
        generator.generate_version()
    for operation_type, valid_from, arguments in generator.operations:
        generator.collection.execute_operation(operation_type, valid_from, arguments)
    setup_operations_s = time.time() - start

    query_set = []
    for _ in range(QUERY_SET_SIZE):
        field_name = random.choice(generator.fields)[0]
        query_set.append({field_name: random.choice(generator.field_domain[field_name])})

    def make_record():
        record = generator.generate_record()
        return json.dumps(record, default=str), record['valid_from_date']

    return CorpusHandle(
        database_name=generator.database_name,
        collection_name=generator.collection_name,
        query_set=query_set,
        setup_insert_s=setup_insert_s,
        # a zero-length chain still costs a measurable sliver; keep it non-zero
        # so downstream division never sees 0.0
        setup_operations_s=max(setup_operations_s, 1e-6),
        record_count=records,
        make_record=make_record,
    )
