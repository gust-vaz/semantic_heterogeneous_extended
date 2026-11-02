from .SemanticOperation import SemanticOperation, OperationSpec
from .exceptions import InvalidOperationArguments


class GroupingOperation(SemanticOperation):
    type_name = 'merging'

    def __init__(self, Collection_):
        super().__init__(Collection_)
        ## query expansion direction: a query for a merged value expands backward
        ## to its sources; expanding an old value forward would over-match
        self.forward_processable = False
        self.backward_processable = True
        ## record reprocessing direction: records evolve forward into the merged
        ## value; backward is impossible (no way to redistribute a merged record)
        self.forward_reapplicable = True
        self.backward_reapplicable = False

    def describe(self, args):
        if 'oldValues' not in args:
            raise InvalidOperationArguments("Missing 'oldValues' parameter for merging")
        if not isinstance(args['oldValues'], list):
            raise InvalidOperationArguments('OldValues argument must be a list')
        if 'newValue' not in args:
            raise InvalidOperationArguments("Missing 'newValue' parameter for merging")
        if 'fieldName' not in args:
            raise InvalidOperationArguments("Missing 'fieldName' parameter for merging")

        return OperationSpec(
            field=args['fieldName'],
            forward_from=args['oldValues'],
            forward_to=args['newValue'],
            cascade_values=[args['newValue'], *args['oldValues']]
        )
