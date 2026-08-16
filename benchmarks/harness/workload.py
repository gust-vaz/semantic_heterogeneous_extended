"""Concurrent load driver.

Each worker owns its own BasicCollection (and therefore its own MongoClient
pool), mirroring independent application clients and avoiding any shared
mutable state inside Collection. Workers run straight through
warmup + measurement; samples taken before the warmup boundary are dropped
afterwards, so no mid-run coordination is needed.
"""

import random
import threading
import time
from dataclasses import dataclass, field

from semantic_heterogeneous_database import BasicCollection

READ = "read"
WRITE = "write"


@dataclass
class ClientSpec:
    mongo_uri: str
    operation_mode: str
    write_concern: str
    read_uri: object = None
    read_mode: str = "split"

    def __post_init__(self):
        # MongoDB reads a *string* "1" as a write-concern mode NAME, not the
        # number 1, and rejects it with UnknownReplWriteConcern - which would
        # silently fail every write in the run. Coerce digit strings to int.
        if isinstance(self.write_concern, str) and self.write_concern.isdigit():
            self.write_concern = int(self.write_concern)


@dataclass
class WorkloadResult:
    latencies_ms: list = field(default_factory=list)
    read_latencies_ms: list = field(default_factory=list)
    write_latencies_ms: list = field(default_factory=list)
    errors: int = 0
    elapsed_s: float = 0.0


def _worker(spec, corpus, read_ratio, deadline, samples, rng):
    """Drive one client until the deadline, appending (timestamp, ms, kind)."""
    try:
        collection = BasicCollection(
            corpus.database_name, corpus.collection_name, spec.mongo_uri,
            spec.operation_mode, write_concern=spec.write_concern,
            read_uri=spec.read_uri, read_mode=spec.read_mode,
        )
    except Exception:
        # An unreachable node must surface as a counted error, not as a thread
        # that dies silently and reports a clean zero-error run.
        samples.append((time.time(), None, None))
        return
    while time.time() < deadline:
        do_read = rng.random() < read_ratio
        started = time.time()
        try:
            if do_read:
                list(collection.find_many(rng.choice(corpus.query_set)))
            else:
                payload, valid_from = corpus.make_record()
                collection.insert_one(payload, valid_from)
            elapsed_ms = (time.time() - started) * 1000.0
            samples.append((started, max(elapsed_ms, 1e-6), READ if do_read else WRITE))
        except Exception:
            samples.append((started, None, None))


def run_workload(client_specs, corpus, read_ratio, warmup_s, measure_s, seed=42):
    """Run all clients for warmup_s + measure_s; report only the measured window."""
    start = time.time()
    boundary = start + warmup_s
    deadline = boundary + measure_s

    per_worker = [[] for _ in client_specs]
    threads = []
    for index, spec in enumerate(client_specs):
        rng = random.Random(seed + index)
        thread = threading.Thread(
            target=_worker,
            args=(spec, corpus, read_ratio, deadline, per_worker[index], rng),
            daemon=True,
        )
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

    result = WorkloadResult(elapsed_s=measure_s)
    for samples in per_worker:
        for timestamp, elapsed_ms, kind in samples:
            if timestamp < boundary:
                continue  # warmup traffic, discarded
            if elapsed_ms is None:
                result.errors += 1
                continue
            result.latencies_ms.append(elapsed_ms)
            if kind == READ:
                result.read_latencies_ms.append(elapsed_ms)
            else:
                result.write_latencies_ms.append(elapsed_ms)
    return result
