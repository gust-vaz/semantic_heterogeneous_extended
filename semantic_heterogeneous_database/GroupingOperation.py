import pandas as pd

from .SemanticOperation import SemanticOperation, OperationSpec
from .exceptions import MellowDBError, InvalidOperationArguments


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

    def check_if_affected(self, Document):
        versions_df = self.collection.versions_df
        return_obj = set()


        original_version = versions_df.loc[versions_df['version_number'] == Document['_original_version']].iloc[0]

        if 'next_operation.type' in versions_df.columns:
            versions_df_p = versions_df.loc[(versions_df['next_operation.type'].isin(['merging', 'grouping'])) & (versions_df['next_version_valid_from'] > original_version['version_valid_from']) & (versions_df['next_version'] <= Document['_max_version_number']) & (versions_df['next_version'] >= Document['_min_version_number']) ]

            if len(versions_df_p) > 0:
                if {'next_operation.type','next_operation.field', 'next_operation.from'}.issubset(versions_df.columns):
                    versions_df_p['field_value'] = versions_df_p.apply(lambda row: Document.get(row['next_operation.field'], None), axis=1)
                    versions_df_p = versions_df_p.loc[versions_df_p.apply(
                        lambda row: row['field_value'] in row['next_operation.from']
                        if isinstance(row['next_operation.from'], list)
                        else row['field_value'] == row['next_operation.from'],
                        axis=1
                    )]

                    if len(versions_df_p) > 0:
                        for ind,v in versions_df_p.iterrows():
                            return_obj.add((float(v['version_number']),float(v['next_version']),'forward'))

        # Agrupamento nao pode andar backwards

        return list(return_obj)

    ## Function is executed when is already known the document suffered changes
    def evolute_forward(self, Document, operation):
        if Document[operation['next_operation.field'].values[0]] in operation['next_operation.from'].values[0]:
            Document = Document.copy()
            Document[operation['next_operation.field'].values[0]] = operation['next_operation.to'].values[0]
            return Document
        else:
            raise MellowDBError('Record should not be evoluted')

    def check_if_many_affected(self, DocumentsDataFrame):
        versions_df = self.collection.versions_df
        return_obj = list()

        ## Merging operation can only be applied forward.

        if 'next_operation.type' in versions_df:
            versions_df_p = self.collection.versions_df.loc[versions_df['next_operation.type'].isin(['merging', 'grouping'])]
            versions_df_p = versions_df_p.explode('next_operation.from') #Abrindo listas dos grupos em linhas diferentes

            if len(versions_df_p) > 0:
                grouped_df = versions_df_p.groupby(by='next_operation.field')

                for field, group in grouped_df:
                    if field not in DocumentsDataFrame.columns: # documents are schemaless; an evolved field may be absent from this batch
                        continue
                    versions_g = versions_df_p.loc[versions_df_p['next_operation.field'] == field]
                    merged_records = pd.merge(DocumentsDataFrame, versions_g, how='left', left_on=field, right_on='next_operation.from')

                    merged_records['match'] = (merged_records['next_operation.field'].notna()) & (merged_records['next_version_valid_from'] > merged_records['_valid_from']) & (merged_records['next_version'] < merged_records['_max_version_number']) & (merged_records['next_version'] >= merged_records['_min_version_number'])
                    matched = merged_records.loc[merged_records['match']]

                    return_obj.append((field, matched, 'forward'))

        return return_obj

    def evolute_many_forward(self, field, DocumentOperationDataFrame):
        d = DocumentOperationDataFrame.copy()
        d[field] = d['next_operation.to']
        return d

    def reapply_operation_forward(self, version_change):
        # A previous evolution has been hit by this new evolution. We need to reprocess it.
        operation = version_change['next_operation']
        self.collection.split_processed_records(
            operation['field'], {'$in': operation['from']},
            version_change['next_version'], operation['to'], 'forward'
        )

        ##Recheck
        self.collection.check_if_operation_affected_forward(operation['field'], operation['to'], version_change['next_version'])#Recheck if affected any other evolution
