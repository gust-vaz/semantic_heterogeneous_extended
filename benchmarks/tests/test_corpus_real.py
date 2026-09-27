import threading

import pytest
from pymongo import MongoClient
from benchmarks.harness.corpus import (
    DATASET_ROOT, dataset_available, build_real, build_corpus,
)

needs_dataset = pytest.mark.skipif(
    not dataset_available(), reason="DATASUS dataset not present"
)


def test_dataset_available_is_false_for_a_missing_root(tmp_path):
    assert dataset_available(str(tmp_path / "nope")) is False


def test_build_real_fails_loudly_when_the_dataset_is_absent(tmp_path, primary_uri):
    with pytest.raises(FileNotFoundError) as exc:
        build_real(primary_uri, operation_mode="preprocess",
                   dataset_root=str(tmp_path / "nope"))
    # the message must name the path so the user knows what to provide
    assert "nope" in str(exc.value)


def test_build_corpus_rejects_an_unknown_kind(primary_uri):
    with pytest.raises(ValueError) as exc:
        build_corpus("imaginary", primary_uri=primary_uri)
    assert "imaginary" in str(exc.value)


def test_build_corpus_dispatches_to_synthetic(primary_uri, cleanup_corpus):
    handle = cleanup_corpus(primary_uri, build_corpus(
        "synthetic", primary_uri=primary_uri, records=10,
        chain_length=0, operation_mode="preprocess"))
    assert handle.record_count == 10


def test_build_corpus_drops_arguments_the_chosen_kind_cannot_use(primary_uri, cleanup_corpus):
    # Experiments pass the union of both kinds' arguments; synthetic must
    # tolerate max_files rather than raising TypeError.
    handle = cleanup_corpus(primary_uri, build_corpus(
        "synthetic", primary_uri=primary_uri, records=10, chain_length=0,
        operation_mode="preprocess", max_files=3))
    assert handle.record_count == 10


@needs_dataset
@pytest.mark.slow
def test_real_corpus_loads_one_file_and_queries_match(primary_uri, cleanup_corpus):
    from semantic_heterogeneous_database import BasicCollection

    handle = cleanup_corpus(primary_uri, build_real(
        primary_uri, operation_mode="preprocess", max_files=1))

    assert handle.record_count > 0
    assert len(handle.query_set) > 0
    collection = BasicCollection(handle.database_name, handle.collection_name,
                                 primary_uri, "preprocess")
    matched = sum(collection.count_documents(q) for q in handle.query_set)
    assert matched > 0


def _mini_dataset(root, distinct_causes=60):
    """The DATASUS layout and headers at a size that loads in about a second: more
    distinct causes than the 50-query sample, and no semantic operations."""
    source = root / "source_data"
    source.mkdir()
    rows = ["UF,municipio,ano,RefDate,cid,ocorrencias"]
    rows += [f"SC,GRAO PARA,1996,1996-12-31,{i:03d} CAUSA {i},1.0" for i in range(distinct_causes)]
    (source / "mortalidade_mini_1996.csv").write_text("\n".join(rows) + "\n")
    operations = root / "semantic_operations"
    operations.mkdir()
    (operations / "operations_cid9_cid10.csv").write_text("from;to;type;valid_from;field\n")
    return str(root)


def test_same_seed_produces_the_same_real_queries_despite_concurrent_traffic(
        tmp_path, primary_uri, cleanup_corpus):
    """The query sample and make_record draw from a seeded generator, and pymongo
    advances the global one with every request it sends, from any thread."""
    root = _mini_dataset(tmp_path)
    stop = threading.Event()

    def chatter():
        client = MongoClient(primary_uri)
        while not stop.is_set():
            client.admin.command("ping")

    thread = threading.Thread(target=chatter, daemon=True)
    thread.start()
    try:
        first = cleanup_corpus(primary_uri, build_real(
            primary_uri, operation_mode="preprocess", dataset_root=root, seed=7))
        second = cleanup_corpus(primary_uri, build_real(
            primary_uri, operation_mode="preprocess", dataset_root=root, seed=7))
        first_records = [first.make_record() for _ in range(5)]
        second_records = [second.make_record() for _ in range(5)]
    finally:
        stop.set()
        thread.join(timeout=5)

    assert len(first.query_set) == 50
    assert first.query_set == second.query_set
    assert first_records == second_records


def test_build_corpus_drops_skew_for_the_real_corpus(tmp_path, primary_uri):
    """Experiments pass the union of both kinds' arguments. The real corpus has
    whatever skew the data has, so the knob must be dropped, not raise TypeError."""
    with pytest.raises(FileNotFoundError):
        build_corpus("real", primary_uri=primary_uri, operation_mode="preprocess",
                     skew=1.5, records=10, dataset_root=str(tmp_path / "nope"))


def test_build_real_accepts_a_shard_key():
    """Without this the DATASUS corpus cannot be sharded at all, and the S series
    needs both corpora."""
    import inspect

    from benchmarks.harness.corpus import build_real
    assert "shard_key" in inspect.signature(build_real).parameters


def test_build_corpus_hands_a_shard_key_to_the_real_corpus(tmp_path, primary_uri):
    from semantic_heterogeneous_database import sharding
    if sharding.is_mongos(MongoClient(primary_uri)):
        pytest.skip("asserts the off-cluster warning")
    root = _mini_dataset(tmp_path)
    with pytest.warns(RuntimeWarning, match="cid"):
        handle = build_corpus("real", primary_uri=primary_uri,
                              operation_mode="preprocess", dataset_root=root,
                              shard_key={"cid": "hashed"})
    MongoClient(primary_uri).drop_database(handle.database_name)
