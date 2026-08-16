"""Corpus construction: populate a collection and hand back a workload's inputs.

Two implementations behind one shape - synthetic (default, runs anywhere) and
real DATASUS. Experiments never know which one they were given.
"""

import json
import os
import random
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from benchmarks.database_generator import DatabaseGenerator
from semantic_heterogeneous_database import BasicCollection

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


#: Worst case a single semantic operation burns 3 domain values (a merge takes
#: two old values plus a new one), and every operation may land on the same
#: field, so this many values per field guarantees generate_version() can always
#: find an unused combination.
DOMAIN_VALUES_PER_OPERATION = 4
MIN_DOMAIN = 20


def domain_for_chain(chain_length):
    """Smallest field domain that can support `chain_length` semantic operations.

    DatabaseGenerator refuses to evolve a value twice, so a domain that is too
    small exhausts its pool of unused values and generate_version() recurses
    until RecursionError.
    """
    return max(MIN_DOMAIN, DOMAIN_VALUES_PER_OPERATION * chain_length)


def build_synthetic(primary_uri, records, chain_length, operation_mode,
                    write_concern="majority", seed=42, fields=8, domain=None,
                    evolution_fields=2):
    """Fabricate a corpus with DatabaseGenerator and apply `chain_length` operations.

    `domain` defaults to whatever this chain length needs. Sweeps that vary
    chain_length should pass one explicit domain sized for their longest chain,
    so query selectivity stays constant across the sweep.
    """
    if domain is None:
        domain = domain_for_chain(chain_length)
    required = domain_for_chain(chain_length)
    if domain < required:
        raise ValueError(
            f"domain={domain} is too small for chain_length={chain_length}; "
            f"DatabaseGenerator would exhaust its unused values. "
            f"Use domain >= {required}."
        )
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


DATASET_ROOT = os.path.join("dataset", "MellowDB_experiments")
SOURCE_SUBDIR = "source_data"
OPERATIONS_FILE = os.path.join("semantic_operations", "operations_cid9_cid10.csv")
EVOLVING_FIELD = "cid"
VALID_FROM_FIELD = "RefDate"


def dataset_available(root=None):
    root = root or DATASET_ROOT
    return os.path.isdir(os.path.join(root, SOURCE_SUBDIR))


def build_real(primary_uri, operation_mode, write_concern="majority",
               max_files=None, dataset_root=None, seed=42):
    """Load the DATASUS mortality corpus and apply the CID-9 -> CID-10 operations."""
    root = dataset_root or DATASET_ROOT
    source = os.path.join(root, SOURCE_SUBDIR)
    if not os.path.isdir(source):
        raise FileNotFoundError(
            f"DATASUS dataset not found at '{source}'. "
            "Provide the dataset or run with --corpus synthetic."
        )

    database_name = 'benchdb_' + uuid.uuid4().hex[:12]
    collection_name = 'col_' + uuid.uuid4().hex[:12]
    collection = BasicCollection(database_name, collection_name, primary_uri,
                                 operation_mode, write_concern=write_concern)

    # insert_many_by_csv ingests every CSV in a folder, so a capped run copies
    # just the first N files into a scratch folder rather than the whole set.
    staging = None
    folder = source
    if max_files is not None:
        staging = tempfile.mkdtemp(prefix="bench_corpus_")
        for name in sorted(os.listdir(source))[:max_files]:
            if name.endswith(".csv"):
                shutil.copy2(os.path.join(source, name), staging)
        folder = staging

    try:
        start = time.time()
        collection.insert_many_by_csv(folder, VALID_FROM_FIELD, '%Y-%m-%d', ',')
        setup_insert_s = time.time() - start
    finally:
        if staging:
            shutil.rmtree(staging, ignore_errors=True)

    start = time.time()
    collection.execute_many_operations_by_csv(
        os.path.join(root, OPERATIONS_FILE), 'type', 'valid_from')
    setup_operations_s = time.time() - start

    client = MongoClient(primary_uri)
    raw = client[database_name][collection_name]
    record_count = raw.count_documents({})

    # Draw queries from the corpus's own values so they actually match records.
    random.seed(seed)
    values = raw.distinct(EVOLVING_FIELD)
    sample = random.sample(values, min(QUERY_SET_SIZE, len(values)))
    query_set = [{EVOLVING_FIELD: value} for value in sample]

    def make_record():
        record = {
            EVOLVING_FIELD: random.choice(values) if values else "unknown",
            "ocorrencias": random.randint(1, 1000),
        }
        return json.dumps(record, default=str), datetime(2010, 1, 1)

    return CorpusHandle(
        database_name=database_name,
        collection_name=collection_name,
        query_set=query_set,
        setup_insert_s=setup_insert_s,
        setup_operations_s=max(setup_operations_s, 1e-6),
        record_count=record_count,
        make_record=make_record,
    )


def build_corpus(kind, **kwargs):
    """Dispatch to the requested corpus implementation.

    Callers pass the union of both implementations' arguments so an experiment
    never has to branch on the corpus kind; each branch drops the ones that do
    not apply to it.
    """
    if kind == "synthetic":
        kwargs.pop("max_files", None)
        return build_synthetic(**kwargs)
    if kind == "real":
        kwargs.pop("records", None)
        kwargs.pop("chain_length", None)
        kwargs.pop("domain", None)
        return build_real(**kwargs)
    raise ValueError(f"Unknown corpus '{kind}'. Choose: synthetic, real")
