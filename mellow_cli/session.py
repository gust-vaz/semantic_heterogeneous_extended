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

    def load_csv(self, folder, date_field="RefDate", delimiter=",", date_format="%Y-%m-%d"):
        self._collection.insert_many_by_csv(folder, date_field, date_format, delimiter)

    def apply_operations_csv(self, file):
        self._collection.execute_many_operations_by_csv(file, "type", "valid_from")

    def apply_operation(self, op_type, valid_from, args):
        self._collection.execute_operation(op_type, valid_from, args)

    def query(self, query):
        return list(self._collection.find_many(query))

    def count(self, query):
        return self._collection.count_documents(query)

    def drop(self):
        self._collection.collection.client.drop_database(self.db)

    def set_read_node(self, node, mode="split"):
        if node == "primary":
            self._collection.set_read_source(None, mode)
            self.read_node = "primary"
        else:
            read_uri = f"mongodb://localhost:{node}/?directConnection=true"
            self._collection.set_read_source(read_uri, mode)
            self.read_node = str(node)
        self.read_mode = mode

    def status(self):
        return {
            "db": self.db,
            "collection": self.collection_name,
            "operation_mode": self.operation_mode,
            "write_concern": self.write_concern,
            "read_node": self.read_node,
            "read_mode": self.read_mode,
        }
