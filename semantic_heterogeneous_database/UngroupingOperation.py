import pandas as pd

from .SemanticOperation import SemanticOperation, OperationSpec
from .exceptions import MellowDBError, InvalidOperationArguments


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

    def check_if_affected(self, Document):
        versions_df = self.collection.versions_df
        return_obj = list()


        if 'previous_operation.type' in versions_df.columns:
            versions_df_p = versions_df.loc[(versions_df['previous_operation.type'].isin(['splitting', 'ungrouping'])) & (versions_df['version_number'] <= Document['_original_version']) & (versions_df['previous_version'] <= Document['_max_version_number']) & (versions_df['previous_version'] > Document['_min_version_number']) ] #Operacao precisa partir de versao igual ou inferior a atual
            versions_df_p = versions_df_p.explode('previous_operation.from')

            if len(versions_df_p) > 0:
                if {'previous_operation.type','previous_operation.field', 'previous_operation.from'}.issubset(versions_df.columns):
                    versions_df_p['field_value'] = versions_df_p.apply(lambda row: Document.get(row['previous_operation.field'], None), axis=1)
                    versions_df_p = versions_df_p.loc[
                        versions_df_p.apply(
                            lambda row: row['field_value'] in row['previous_operation.from']
                            if isinstance(row['previous_operation.from'], list)
                            else row['field_value'] == row['previous_operation.from'],
                            axis=1
                        )
                    ]

                    versions_df_p.sort_values('version_number', inplace=True)

                    if len(versions_df_p) > 0:
                        return_obj.append((float(versions_df_p.iloc[0]['version_number']),float(versions_df_p.iloc[0]['previous_version']),'backward'))


                    ## Splitting cannot be applied forward
        return list(return_obj)

    def evolute_backward(self, Document, operation):
        if Document[operation['previous_operation.field'].values[0]] == operation['previous_operation.from'].values[0]:
            Document = Document.copy()
            Document[operation['previous_operation.field'].values[0]] = operation['previous_operation.to'].values[0]
            return Document
        else:
            raise MellowDBError('Record should not be evoluted')

    def check_if_many_affected(self, DocumentsDataFrame):
        versions_df = self.collection.versions_df
        return_obj = list()

        ##Splitting can only be applied backwards

        if 'previous_operation.type' in versions_df:
            versions_df_p = self.collection.versions_df.loc[versions_df['previous_operation.type'].isin(['splitting', 'ungrouping'])]
            versions_df_p = versions_df_p.explode('previous_operation.from')

            if len(versions_df_p) > 0:
                grouped_df = versions_df_p.groupby(by='previous_operation.field')

                for field, group in grouped_df:
                    if field not in DocumentsDataFrame.columns: # documents are schemaless; an evolved field may be absent from this batch
                        continue
                    versions_g = versions_df_p.loc[versions_df_p['previous_operation.field'] == field]
                    merged_records = pd.merge(DocumentsDataFrame, versions_g, how='left', left_on=field, right_on='previous_operation.from')

                    merged_records['match'] = (merged_records['previous_operation.field'].notna()) & (merged_records['version_number'] <= merged_records['_original_version']) & (merged_records['previous_version'] <= merged_records['_max_version_number']) & (merged_records['previous_version'] > merged_records['_min_version_number'])
                    matched = merged_records.loc[merged_records['match']]

                    return_obj.append((field, matched, 'backward'))

        return return_obj

    def evolute_many_backward(self, field, DocumentOperationDataFrame):
        d = DocumentOperationDataFrame.copy()
        d[field] = d['previous_operation.to']
        return d

    def reapply_operation_backward(self, version_change):
        # A previous evolution has been hit by this new evolution. We need to reprocess it.
        operation = version_change['previous_operation']
        self.collection.split_processed_records(
            operation['field'], {'$in': operation['from']},
            version_change['previous_version'], operation['to'], 'backward',
            BoundaryVersion=version_change['version_number']
        )

        ##Recheck
        self.collection.check_if_operation_affected_backward(operation['field'], operation['to'], version_change['previous_version'])#Recheck if affected any other evolution
