# tests/distributed/test_connection.py
from semantic_heterogeneous_database import BasicCollection

def test_collection_accepts_full_uri():
    """Collection.__init__ accepts a full mongodb:// URI."""
    col = BasicCollection(
        "test_uri_db", "col",
        mongo_uri="mongodb://localhost:27017",
        operation_mode="preprocess"
    )
    col.collection.client.drop_database("test_uri_db")

def test_collection_positional_host_still_works():
    """Passing host as positional arg (backward compat) still connects."""
    col = BasicCollection("test_pos_db", "col", "localhost", "preprocess")
    col.collection.client.drop_database("test_pos_db")
