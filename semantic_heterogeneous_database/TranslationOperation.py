from .SemanticOperation import SemanticOperation, OperationSpec
from .exceptions import InvalidOperationArguments


class TranslationOperation(SemanticOperation):
    type_name = 'translation'

    def __init__(self, Collection_):
        super().__init__(Collection_)
        ## query expansion direction
        self.forward_processable = True
        self.backward_processable = True
        ## record reprocessing direction (reapply_operation_*)
        self.forward_reapplicable = True
        self.backward_reapplicable = True

    def describe(self, args):
        if 'oldValue' not in args:
            raise InvalidOperationArguments("Missing 'oldValue' parameter for translation")
        if 'newValue' not in args:
            raise InvalidOperationArguments("Missing 'newValue' parameter for translation")
        if 'fieldName' not in args:
            raise InvalidOperationArguments("Missing 'fieldName' parameter for translation")

        return OperationSpec(
            field=args['fieldName'],
            forward_from=args['oldValue'],
            forward_to=args['newValue'],
            cascade_values=[args['newValue'], args['oldValue']]
        )

    def reapply_operation_backward(self, version_change):
        # A previous evolution has been hit by this new evolution. We need to reprocess it.
        operation = version_change['previous_operation']
        self.collection.split_processed_records(
            operation['field'], operation['from'],
            version_change['version_number'], operation['to'], 'backward',
            version_change['version_valid_from']
        )

        ##Recheck
        self.collection.check_if_operation_affected_backward(operation['field'], operation['to'], version_change['previous_version'])#Recheck if affected any other evolution
