from .SemanticOperation import SemanticOperation, OperationSpec
from .exceptions import InvalidOperationArguments


class UngroupingOperation(SemanticOperation):
    type_name = 'splitting'

    def __init__(self, Collection_):
        super().__init__(Collection_)
        ## query expansion direction: a query for a pre-split value expands forward
        ## to its fragments; expanding a fragment backward would over-match
        self.forward_processable = True
        self.backward_processable = False
        ## record reprocessing direction: records evolve backward into the pre-split
        ## value; forward is impossible (no way to pick a fragment for a record)
        self.forward_reapplicable = False
        self.backward_reapplicable = True

    def describe(self, args):
        if 'oldValue' not in args:
            raise InvalidOperationArguments("Missing 'oldValue' parameter for splitting")
        if 'newValues' not in args:
            raise InvalidOperationArguments("Missing 'newValues' parameter for splitting")
        if not isinstance(args['newValues'], list):
            raise InvalidOperationArguments('NewValues argument must be a list')
        if 'fieldName' not in args:
            raise InvalidOperationArguments("Missing 'fieldName' parameter for splitting")

        return OperationSpec(
            field=args['fieldName'],
            forward_from=args['oldValue'],
            forward_to=args['newValues'],
            cascade_values=[args['oldValue'], *args['newValues']]
        )

    def reapply_operation_backward(self, version_change):
        # A previous evolution has been hit by this new evolution. We need to reprocess it.
        operation = version_change['previous_operation']
        self.collection.split_processed_records(
            operation['field'], {'$in': operation['from']},
            version_change['version_number'], operation['to'], 'backward',
            version_change['version_valid_from']
        )

        ##Recheck
        self.collection.check_if_operation_affected_backward(operation['field'], operation['to'], version_change['previous_version'])#Recheck if affected any other evolution
