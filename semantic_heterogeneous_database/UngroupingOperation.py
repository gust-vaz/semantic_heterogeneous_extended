import random
import uuid
import pandas as pd
from  .SemanticOperation import SemanticOperation
import datetime
from argparse import ArgumentError
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern

class UngroupingOperation:
    def __init__(self, Collection_):
        self.collection = Collection_.collection
        ## query expansion direction: a query for a pre-split value expands forward
        ## to its fragments; expanding a fragment backward would over-match
        self.forward_processable = True
        self.backward_processable = False
        ## record reprocessing direction: records evolve backward into the pre-split
        ## value; forward is impossible (no way to pick a fragment for a record)
        self.forward_reapplicable = False
        self.backward_reapplicable = True

    def execute_operation(self, validFromDate: datetime, args: dict):
        if 'oldValue' not in args:
            raise ArgumentError("oldValue", "Missing 'oldValue' parameter for splitting")
        if 'newValues' not in args:
            raise ArgumentError("newValues", "Missing 'newValues' parameter for splitting")
        if not isinstance(args['newValues'], list):
            raise ArgumentError('newValues', 'NewValues argument must be a list')
        if 'fieldName' not in args:
            raise ArgumentError("fieldName", "Missing 'fieldName' parameter for splitting")

        oldValue = args['oldValue']
        newValues = args['newValues']
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
            # Backward: records carrying any of newValues before the split date
            temporary_collection = str(uuid.uuid4())
            self.collection.collection_processed.aggregate([
                {'$match': {'$and': [
                    {'_min_version_number': {'$lte': new_version_number}},
                    {'_max_version_number': {'$gt': new_version_number}},
                    {fieldName: {'$in': newValues}}
                ]}},
                {'$unset': '_id'},
                {'$out': temporary_collection}
            ])
            self.collection.collection_processed.update_many(
                {'$and': [
                    {'_min_version_number': {'$lte': new_version_number}},
                    {'_max_version_number': {'$gt': new_version_number}},
                    {fieldName: {'$in': newValues}}
                ]},
                {'$set': {'_min_version_number': new_version_number}}
            )
            self.collection.db[temporary_collection].update_many(
                {}, {'$set': {'_max_version_number': new_version_number, fieldName: oldValue}}
            )
            self.collection.db[temporary_collection].aggregate([
                {'$match': {}},
                {'$merge': {'into': self.collection.collection_processed.name, 'whenMatched': 'fail'}}
            ])
            self.collection.db[temporary_collection].drop()

        new_version = {
            "current_version": 1 if next_version_count == 0 else 0,
            "version_valid_from": validFromDate,
            "previous_version": previous_version['version_number'],
            "previous_version_valid_from": previous_version['version_valid_from'],
            "previous_operation": {
                "type": "splitting",
                "field": fieldName,
                "from": newValues,
                "to": oldValue
            },
            "next_version": None,
            "next_operation": None,
            "version_number": new_version_number
        }
        next_operation = {
            "type": "splitting",
            "field": fieldName,
            "from": oldValue,
            "to": newValues
        }
        if next_version is not None and 'version_valid_from' in next_version:
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
                raise RuntimeError(
                    "Version chain conflict: another operation already appended to this position. Retry required."
                )
            if next_version is not None and 'version_valid_from' in next_version:
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
            if next_version is not None:
                self.collection.collection_processed.update_many(
                    {'_evolution_list': previous_version['_id']},
                    {'$push': {'_evolution_list': i.inserted_id}}
                )
            self.collection.check_if_operation_affected_forward(fieldName, oldValue, new_version_number)
            self.collection.check_if_operation_affected_backward(fieldName, oldValue, new_version_number)
            for value in newValues:
                self.collection.check_if_operation_affected_forward(fieldName, value, new_version_number)
                self.collection.check_if_operation_affected_backward(fieldName, value, new_version_number)


    def check_if_affected(self, Document):
        versions_df = self.collection.versions_df
        return_obj = list()

        original_version = versions_df.loc[versions_df['version_number'] == Document['_original_version']].iloc[0]
                
        if 'previous_operation.type' in versions_df.columns:
            versions_df_p = versions_df.loc[(versions_df['previous_operation.type'].isin(['splitting', 'ungrouping']))& (versions_df['previous_version_valid_from'] < original_version['version_valid_from']) & (versions_df['previous_version'] <= Document['_max_version_number']) & (versions_df['previous_version'] >= Document['_min_version_number']) ] #Operacao precisa partir de versao igual ou inferior a atual            
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
            raise BaseException('Record should not be evoluted')


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

                    merged_records['match'] = (merged_records['previous_operation.field'].notna()) & (merged_records['previous_version_valid_from'] < merged_records['_valid_from']) & (merged_records['previous_version'] <= merged_records['_max_version_number']) & (merged_records['previous_version'] >= merged_records['_min_version_number'])
                    matched = merged_records.loc[merged_records['match']]                    
                    
                    return_obj.append((field, matched, 'backward'))      

        return return_obj      

    def evolute_many_backward(self, field, DocumentOperationDataFrame):
        d = DocumentOperationDataFrame.copy()
        d[field] = d['previous_operation.to']
        return d

    def reapply_operation_backward(self, version_change):
        temporary_collection = str(uuid.uuid4())
        #Copying all records affected by the splitting to a temporary collection
        res = self.collection.collection_processed.aggregate([{ '$match': {'$and': [
                                                                        {'_min_version_number' : {'$lte' : version_change['previous_version']}},
                                                                        {'_max_version_number' : {'$gt' : version_change['previous_version']}},
                                                                        {version_change['previous_operation']['field'] : {'$in' : version_change['previous_operation']['from']}}
                                                                    ]
                                                        }
                                            },
                                            {'$unset': '_id'},
                                            { '$out' : temporary_collection } ])

        ##part 1 of split - old registers are cut until last version before splitting
        res = self.collection.collection_processed.update_many({'$and': [
                                                                {'_min_version_number' : {'$lte' : version_change['previous_version']}},
                                                                {'_max_version_number' : {'$gt' : version_change['previous_version']}},
                                                                {version_change['previous_operation']['field'] : {'$in' : version_change['previous_operation']['from']}}
                                                    ]
                                                    },
                                                    {'$set' : {'_min_version_number' : version_change['version_number']}}
                                                )

        ##part 2 of split - inserting registers starting from new version. Therefore, in the end of the process, records
        #have been splitted in two parts.
        res = self.collection.db[temporary_collection].update_many({},
                                            {'$set' : {'_max_version_number' : version_change['version_number'], version_change['previous_operation']['field'] : version_change['previous_operation']['to']}}
                                            )

        res = self.collection.db[temporary_collection].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection.collection_processed.name, 'whenMatched' : 'fail'}}])
        self.collection.db[temporary_collection].drop()

        self.collection.check_if_operation_affected_backward(version_change['previous_operation']['field'], version_change['previous_operation']['to'],version_change['previous_version'])#Recheck if affected any other evolution
