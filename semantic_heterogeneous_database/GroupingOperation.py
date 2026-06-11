import random
import uuid
import pandas as pd
import datetime
from .exceptions import MellowDBError, InvalidOperationArguments, VersionChainConflict
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern

class GroupingOperation:
    def __init__(self, Collection_):
        self.collection = Collection_.collection
        ## query expansion direction: a query for a merged value expands backward
        ## to its sources; expanding an old value forward would over-match
        self.forward_processable = False
        self.backward_processable = True
        ## record reprocessing direction: records evolve forward into the merged
        ## value; backward is impossible (no way to redistribute a merged record)
        self.forward_reapplicable = True
        self.backward_reapplicable = False

    def execute_operation(self, validFromDate: datetime, args: dict):
        if 'oldValues' not in args:
            raise InvalidOperationArguments("Missing 'oldValues' parameter for merging")
        if not isinstance(args['oldValues'], list):
            raise InvalidOperationArguments('OldValues argument must be a list')
        if 'newValue' not in args:
            raise InvalidOperationArguments("Missing 'newValue' parameter for merging")
        if 'fieldName' not in args:
            raise InvalidOperationArguments("Missing 'fieldName' parameter for merging")

        oldValues = args['oldValues']
        newValue = args['newValue']
        fieldName = args['fieldName']

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

        if self.collection.operation_mode == 'preprocess':
            # Forward only — grouping has no backward preprocessing
            self.collection.split_processed_records(fieldName, {'$in': oldValues}, new_version_number, newValue, 'forward')

        new_version = {
            "current_version": 1 if next_version_count == 0 else 0,
            "version_valid_from": validFromDate,
            "previous_version": previous_version['version_number'],
            "previous_version_valid_from": previous_version['version_valid_from'],
            "previous_operation": {
                "type": "merging",
                "field": fieldName,
                "from": newValue,
                "to": oldValues
            },
            "next_version": None,
            "next_operation": None,
            "version_number": new_version_number
        }
        next_operation = {
            "type": "merging",
            "field": fieldName,
            "from": oldValues,
            "to": newValue
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
            self.collection.check_if_operation_affected_forward(fieldName, newValue, new_version_number)
            self.collection.check_if_operation_affected_backward(fieldName, newValue, new_version_number)
            for value in oldValues:
                self.collection.check_if_operation_affected_forward(fieldName, value, new_version_number)
                self.collection.check_if_operation_affected_backward(fieldName, value, new_version_number)

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
        

    def evolute_backward(self, Document, operation):        
        pass

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