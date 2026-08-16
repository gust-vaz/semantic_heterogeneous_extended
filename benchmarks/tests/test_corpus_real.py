import pytest
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
