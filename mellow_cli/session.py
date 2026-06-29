from datetime import datetime
from semantic_heterogeneous_database import BasicCollection


class MellowSession:
    """Shared engine: wraps one BasicCollection and exposes operator actions."""

    def __init__(self, mongo_uri, db, collection="records",
                 operation_mode="preprocess", write_concern="majority",
                 read_uri=None, read_mode="split"):
        self.mongo_uri = mongo_uri
        self.db = db
        self.collection_name = collection
        self.operation_mode = operation_mode
        self.write_concern = write_concern
        self.read_mode = read_mode
        self.read_node = "primary" if read_uri is None else read_uri
        self._collection = BasicCollection(
            db, collection, mongo_uri, operation_mode,
            write_concern=write_concern, read_uri=read_uri, read_mode=read_mode,
        )

    def insert(self, json_string, valid_from):
        self._collection.insert_one(json_string, valid_from)

    def query(self, query):
        return list(self._collection.find_many(query))

    def count(self, query):
        return self._collection.count_documents(query)

    def drop(self):
        self._collection.collection.client.drop_database(self.db)
