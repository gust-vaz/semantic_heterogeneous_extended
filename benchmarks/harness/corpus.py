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
    #: Operation rows build_real left out of the setup (see `defer_field`), for the
    #: caller to apply and time with apply_deferred_operations.
    deferred_operations: list = field(repr=False, default_factory=list)

    @property
    def deferred_values(self):
        """Every value the deferred operations name, on either side."""
        return ({row["from"] for row in self.deferred_operations}
                | {row["to"] for row in self.deferred_operations})


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
                    evolution_fields=2, shard_key=None, skew=0.0, chained=False):
    """Fabricate a corpus with DatabaseGenerator and apply `chain_length` operations.

    `domain` defaults to whatever this chain length needs. Sweeps that vary
    chain_length should pass one explicit domain sized for their longest chain,
    so query selectivity stays constant across the sweep.

    By default the operations are independent (generate_version never reuses a
    value) and queries are drawn over every field, so few of them touch an evolved
    value. `chained=True` instead builds one linear translation chain of
    `chain_length` hops and draws every query from the chain's values, so query
    resolution has to follow the whole chain - what a version-chain depth sweep
    means. The default path draws exactly what it always did.
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
    rng = random.Random(seed)
    generator = DatabaseGenerator(host=primary_uri, write_concern=write_concern,
                                  shard_key=shard_key, rng=rng, skew=skew)
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
    if chained:
        chain_field, chain_values = generator.generate_chain(chain_length)
    else:
        for _ in range(chain_length):
            generator.generate_version()
    for operation_type, valid_from, arguments in generator.operations:
        generator.collection.execute_operation(operation_type, valid_from, arguments)
    setup_operations_s = time.time() - start

    query_set = []
    for _ in range(QUERY_SET_SIZE):
        if chained:
            query_set.append({chain_field: rng.choice(chain_values)})
            continue
        field_name = rng.choice(generator.fields)[0]
        query_set.append({field_name: rng.choice(generator.field_domain[field_name])})

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
OPERATIONS_SEPARATOR = ";"

#: The years real_transition loads, inclusive. Every cid operation is dated
#: 1996-01-01, the switch from CID-9 to CID-10, so a window around it keeps all
#: of them at work: CID-9 records before it evolve forward, CID-10 records after
#: it are what old terms find. 1988-2003 is the symmetric window closest to a
#: third of the corpus - 16 of 43 files, 1,988,581 of 6,179,083 rows (32.2%),
#: 43% of them CID-9 - and it names 116 of the 119 CID-9 source codes the
#: operations use and every CID-10 target the data has.
TRANSITION_YEARS = (1988, 2003)

#: real_transition samples queries from each of these, so every shard key S3
#: tries - the evolving cid, the also-evolving municipio, the untouched UF - has
#: queries that name it.
TRANSITION_QUERY_FIELDS = ("cid", "municipio", "UF")


def dataset_available(root=None):
    root = root or DATASET_ROOT
    return os.path.isdir(os.path.join(root, SOURCE_SUBDIR))


def transition_files(source, years=TRANSITION_YEARS):
    """The CSVs in `source` whose year (the name's last _-separated part) is in `years`."""
    first, last = years
    names = []
    for name in sorted(os.listdir(source)):
        stem, extension = os.path.splitext(name)
        year = stem.rsplit("_", 1)[-1]
        if extension == ".csv" and year.isdigit() and first <= int(year) <= last:
            names.append(name)
    return names


def apply_deferred_operations(collection, handle):
    """Apply the operations build_real deferred, through the same CSV path it uses."""
    staging = tempfile.mkdtemp(prefix="bench_operations_")
    path = os.path.join(staging, "operations.csv")
    try:
        pd.DataFrame(handle.deferred_operations,
                     columns=["from", "to", "type", "valid_from", "field"]).to_csv(
            path, sep=OPERATIONS_SEPARATOR, index=False)
        collection.execute_many_operations_by_csv(path, 'type', 'valid_from')
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def build_real(primary_uri, operation_mode, write_concern="majority",
               max_files=None, dataset_root=None, seed=42, shard_key=None,
               index=False, files=None, query_fields=(EVOLVING_FIELD,),
               defer_field=None, index_before=()):
    """Load the DATASUS mortality corpus and apply the CID-9 -> CID-10 operations.

    `index=True` then indexes the queried field through the library's own
    create_index (the processed collection under preprocess, the original one
    under rewrite). Without it every query scans the whole corpus: at all 43
    files that is ~10M processed documents and ~60 s per read. Built after the
    operations, so setup_operations_s is unchanged; its time is only logged.

    `files` names the CSVs to load and overrides `max_files`. `query_fields`
    are the fields the query set samples, up to QUERY_SET_SIZE values each.
    `defer_field` leaves that field's operations out of the setup and hands them
    back on the handle, for an experiment that measures applying them.
    `index_before` indexes those fields after the insert and before the
    operations, which then find the records they evolve without a full scan.
    """
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
                                 operation_mode, write_concern=write_concern,
                                 shard_key=shard_key)

    # insert_many_by_csv ingests every CSV in a folder, so a capped run copies
    # just the first N files into a scratch folder rather than the whole set.
    staging = None
    folder = source
    if files is None and max_files is not None:
        files = [name for name in sorted(os.listdir(source))[:max_files]
                 if name.endswith(".csv")]
    if files is not None:
        staging = tempfile.mkdtemp(prefix="bench_corpus_")
        for name in files:
            shutil.copy2(os.path.join(source, name), staging)
        folder = staging

    try:
        start = time.time()
        collection.insert_many_by_csv(folder, VALID_FROM_FIELD, '%Y-%m-%d', ',')
        setup_insert_s = time.time() - start
    finally:
        if staging:
            shutil.rmtree(staging, ignore_errors=True)

    if index_before:
        start = time.time()
        for index_field in index_before:
            collection.create_index([index_field])
        print(f"[corpus] indexes on {', '.join(index_before)} built in "
              f"{time.time() - start:.1f} s", flush=True)

    operations_path = os.path.join(root, OPERATIONS_FILE)
    deferred = []
    if defer_field is not None:
        operations = pd.read_csv(operations_path, sep=OPERATIONS_SEPARATOR, dtype=str,
                                 keep_default_na=False)
        deferred = operations[operations["field"] == defer_field].to_dict("records")
        # Only what predates the deferred operations, so they still land at the end
        # of the chain. Behind a later-dated operation they would land mid-chain,
        # and the library would re-derive every connected record instead - a scan
        # per batch on 2M documents, and not what a new operation arriving costs.
        # ISO dates compare correctly as strings.
        first = min((row["valid_from"] for row in deferred), default=None)
        kept = operations[(operations["field"] != defer_field)
                          & ((first is None) | (operations["valid_from"] < first))]
        operations_staging = tempfile.mkdtemp(prefix="bench_operations_")
        operations_path = os.path.join(operations_staging, "operations.csv")
        kept.to_csv(operations_path, sep=OPERATIONS_SEPARATOR, index=False)

    start = time.time()
    try:
        collection.execute_many_operations_by_csv(operations_path, 'type', 'valid_from')
    finally:
        if defer_field is not None:
            shutil.rmtree(operations_staging, ignore_errors=True)
    setup_operations_s = time.time() - start

    if index:
        start = time.time()
        collection.create_index([EVOLVING_FIELD])
        print(f"[corpus] index on {EVOLVING_FIELD} built in {time.time() - start:.1f} s",
              flush=True)

    client = MongoClient(primary_uri)
    raw = client[database_name][collection_name]
    record_count = raw.count_documents({})

    # Draw queries from the corpus's own values so they actually match records, from a
    # private generator: pymongo draws request ids from the global random module.
    rng = random.Random(seed)
    query_set = []
    for query_field in query_fields:
        candidates = raw.distinct(query_field)
        sample = rng.sample(candidates, min(QUERY_SET_SIZE, len(candidates)))
        query_set += [{query_field: value} for value in sample]
    values = raw.distinct(EVOLVING_FIELD)

    def make_record():
        record = {
            EVOLVING_FIELD: rng.choice(values) if values else "unknown",
            "ocorrencias": rng.randint(1, 1000),
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
        deferred_operations=deferred,
    )


def build_corpus(kind, **kwargs):
    """Dispatch to the requested corpus implementation.

    Callers pass the union of both implementations' arguments so an experiment
    never has to branch on the corpus kind; each branch drops the ones that do
    not apply to it.
    """
    if kind == "synthetic":
        kwargs.pop("max_files", None)
        kwargs.pop("defer_field", None)
        return build_synthetic(**kwargs)
    if kind in REAL_KINDS:
        kwargs["index"] = kind != "real"
        kwargs.pop("records", None)
        kwargs.pop("chain_length", None)
        kwargs.pop("domain", None)
        kwargs.pop("skew", None)
        kwargs.pop("chained", None)   # the real corpus brings its own operations
        if kind == "real_transition":
            # The window replaces the profile's file cap: a capped prefix of the
            # sorted names is all CID-10, with nothing for the operations to evolve.
            kwargs.pop("max_files", None)
            source = os.path.join(kwargs.get("dataset_root") or DATASET_ROOT, SOURCE_SUBDIR)
            if os.path.isdir(source):
                kwargs["files"] = transition_files(source)
            kwargs["query_fields"] = TRANSITION_QUERY_FIELDS
            # Indexed before the operations: unindexed, each of the 70 municipio
            # operations scans 2M processed documents several times, about two
            # minutes apiece on sh1. Every query field gets one, so no S3
            # comparison sets an indexed arm against an unindexed one.
            kwargs["index"] = False
            kwargs["index_before"] = TRANSITION_QUERY_FIELDS
        return build_real(**kwargs)
    raise ValueError(f"Unknown corpus '{kind}'. Choose: {', '.join(CORPUS_KINDS)}")


REAL_KINDS = ("real", "real_indexed", "real_transition")
CORPUS_KINDS = ("synthetic",) + REAL_KINDS
