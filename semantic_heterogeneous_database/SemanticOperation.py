import datetime
import random

from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern

from .exceptions import UnsupportedDirectionError, VersionChainConflict


class OperationSpec:
    """Declarative description of one semantic evolution operation, produced by
    SemanticOperation.describe(). The forward direction transforms forward_from
    into forward_to; the backward direction is the mirror image. A list on
    either side means that side is ambiguous when used as an evolution target,
    so the corresponding record-preprocessing direction is skipped.
    """
    def __init__(self, field, forward_from, forward_to, cascade_values):
        self.field = field
        self.forward_from = forward_from
        self.forward_to = forward_to
        self.cascade_values = cascade_values  # values whose pre-existing evolutions must be rechecked, in order


def _as_filter(value):
    return {'$in': value} if isinstance(value, list) else value


class SemanticOperation:
    """Base class for semantic evolution operations.

    Subclasses must set:
      - type_name: the operation type string stored in the versions collection
      - forward_processable / backward_processable: query expansion directions
      - forward_reapplicable / backward_reapplicable: record reprocessing directions
    and implement describe(), check_if_affected(), check_if_many_affected(),
    plus the evolute_*/reapply_* methods of their supported directions.
    Unsupported directions inherit a loud UnsupportedDirectionError default.
    """

    type_name = None

    def __init__(self, Collection_):
        self.collection = Collection_.collection

    def describe(self, args) -> OperationSpec:
        """Validate args and return the OperationSpec for this operation."""
        raise NotImplementedError

    def check_if_affected(self, Document):
        raise NotImplementedError

    def check_if_many_affected(self, DocumentsDataFrame):
        raise NotImplementedError

    def evolute_forward(self, Document, operation):
        raise UnsupportedDirectionError(f'{self.type_name} cannot evolve records forward')

    def evolute_backward(self, Document, operation):
        raise UnsupportedDirectionError(f'{self.type_name} cannot evolve records backward')

    def evolute_many_forward(self, field, DocumentOperationDataFrame):
        raise UnsupportedDirectionError(f'{self.type_name} cannot evolve records forward')

    def evolute_many_backward(self, field, DocumentOperationDataFrame):
        raise UnsupportedDirectionError(f'{self.type_name} cannot evolve records backward')

    def reapply_operation_forward(self, version_change):
        raise UnsupportedDirectionError(f'{self.type_name} cannot be reapplied forward')

    def reapply_operation_backward(self, version_change):
        raise UnsupportedDirectionError(f'{self.type_name} cannot be reapplied backward')

    def execute_operation(self, validFromDate: datetime.datetime, args: dict):
        """Register the operation in the version chain and preprocess affected records.
        The flow is identical for every operation type; only the OperationSpec differs.
        """
        spec = self.describe(args)
        fieldName = spec.field

        # --- Read prev/next once to determine the version boundary for preprocessing ---
        previous_version = self.collection._versions_r.find_one(
            {'version_valid_from': {'$lt': validFromDate}},
            sort=[('version_valid_from', -1), ('version_number', -1)]
        )
        next_version_count = self.collection._versions_r.count_documents(
            {'version_valid_from': {'$gte': validFromDate}}
        )

        if next_version_count > 0:
            next_versions = self.collection._versions_r.find(
                {'version_valid_from': {'$gte': validFromDate}}
            ).sort([('version_valid_from', 1), ('version_number', 1)])
            next_version = next(next_versions, None)
            if next_version and next_version['version_valid_from'] == validFromDate:
                same_date_versions = [next_version] + [
                    v for v in next_versions if v['version_valid_from'] == validFromDate
                ]
                if len(same_date_versions) > 1:
                    next_version = random.choice(same_date_versions)
                    previous_version = self.collection._versions_r.find_one(
                        {'version_number': next_version['previous_version']},
                        sort=[('version_valid_from', -1), ('version_number', -1)]
                    )
            new_version_number = random.uniform(
                previous_version['version_number'], next_version['version_number']
            )
        else:
            next_version = {'version_number': float('inf')}
            self.collection.current_version = self.collection.current_version + 1_000_000
            new_version_number = self.collection.current_version

        # --- Preprocessing ($out cannot run in a transaction) ---
        # A direction is only record-processable when its evolution target is a
        # single value; a list target (merged sources, split fragments) is ambiguous.
        if self.collection.operation_mode == 'preprocess':
            if not isinstance(spec.forward_from, list):
                # Backward: records already carrying the newer value before the evolution date
                self.collection.split_processed_records(
                    fieldName, _as_filter(spec.forward_to), new_version_number, spec.forward_from, 'backward')
            if not isinstance(spec.forward_to, list):
                # Forward: records carrying the older value that evolve after the date
                self.collection.split_processed_records(
                    fieldName, _as_filter(spec.forward_from), new_version_number, spec.forward_to, 'forward')

        # --- Version chain update (transaction-protected on replica sets) ---
        new_version = {
            "current_version": 1 if next_version_count == 0 else 0,
            "version_valid_from": validFromDate,
            "previous_version": previous_version['version_number'],
            "previous_version_valid_from": previous_version['version_valid_from'],
            "previous_operation": {
                "type": self.type_name,
                "field": fieldName,
                "from": spec.forward_to,
                "to": spec.forward_from
            },
            "next_version": None,
            "next_operation": None,
            "version_number": new_version_number
        }
        next_operation = {
            "type": self.type_name,
            "field": fieldName,
            "from": spec.forward_from,
            "to": spec.forward_to
        }
        if 'version_valid_from' in next_version:
            new_version['next_version'] = next_version['version_number']
            new_version['next_version_valid_from'] = next_version['version_valid_from']
            new_version['next_operation'] = previous_version['next_operation']

        def _write_version_chain(session=None):
            filter_ = {'version_number': previous_version['version_number']}
            if next_version_count == 0:
                filter_['next_version'] = None  # only match if still the terminal node
            result = self.collection._col_versions_w.update_one(
                filter_,
                {'$set': {
                    'next_operation': next_operation,
                    'next_version': new_version_number,
                    'next_version_valid_from': validFromDate,
                    'current_version': 0
                }},
                session=session
            )
            if next_version_count == 0 and result.modified_count == 0:
                raise VersionChainConflict(
                    "Version chain conflict: another operation already appended to this position. Retry required."
                )
            if 'version_valid_from' in next_version:
                self.collection._col_versions_w.update_one(
                    {'version_number': next_version['version_number']},
                    {'$set': {'previous_version': new_version_number}},
                    session=session
                )
            return self.collection._col_versions_w.insert_one(new_version, session=session)

        if self.collection._is_replica_set:
            with self.collection.client.start_session() as session:
                with session.start_transaction(
                    write_concern=WriteConcern(w='majority'),
                    read_concern=ReadConcern('snapshot')
                ):
                    i = _write_version_chain(session=session)
        else:
            i = _write_version_chain()

        self.collection.update_versions()

        if self.collection.operation_mode == 'preprocess':
            self.collection.collection_processed.update_many(
                {'_evolution_list': previous_version['_id']},
                {'$push': {'_evolution_list': i.inserted_id}}
            )
            for value in spec.cascade_values:
                self.collection.check_if_operation_affected_forward(fieldName, value, new_version_number)
                self.collection.check_if_operation_affected_backward(fieldName, value, new_version_number)
