import time
import uuid
import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING,DESCENDING
from bson.objectid import ObjectId
from datetime import datetime
import json
import csv
import re

class Collection:
    def __init__ (self,DatabaseName, CollectionName, Host='localhost', operation_mode='preprocess'):       

        if not isinstance(operation_mode, str) or operation_mode not in ['preprocess','rewrite']:
            raise BaseException('Operation Mode not recognized')
        
        self.operation_mode = operation_mode        
        self.operations = {}
        self.logic_operators = ['$or','$and','$xor','$not','$nor']    

        self.client = MongoClient(Host)        
        self.database_name = DatabaseName
        self.db = self.client[DatabaseName]

        existing_collections = self.db.list_collection_names()
        if operation_mode == 'rewrite':
            if (CollectionName + '_processed') in existing_collections:
                raise BaseException('Previous preprocessed collection already exists.')            
        else:
            if (CollectionName + '_processed') not in existing_collections and CollectionName in existing_collections:
                raise BaseException('Previous rewrite collection already exists.')            


        self.collection = self.db[CollectionName]
        self.collection_versions = self.db[CollectionName + '_versions']

        if operation_mode == 'preprocess':
            self.collection_processed = self.db[CollectionName+'_processed']
            if CollectionName+'_processed' not in existing_collections:
                self.db.create_collection(CollectionName+'_processed')
                
            self.collection_columns = self.db[CollectionName+'_columns']

            ## Loading columns collection in memory        
            fields = self.collection_columns.find({})
            self.fields = dict([(col['field_name'], (col['first_edit_version'], col['last_edit_version'])) for col in fields])

            ## Loading semantic operations in memory       
            self.update_versions()
        
        self.current_version = self.collection_versions.find_one({"current_version":1})
        self.semantic_operations = {}
        
        #initializing first version
        if(self.current_version == None):
            self.current_version = 100
           
            first_version = {
                "current_version":1,
                "version_valid_from":datetime(1700,1,1),
                "previous_version":None, 
                "previous_operation":None,
                "version_number":100, 
                "next_version":None,
                "next_operation":None
            }
            self.collection_versions.insert_one(first_version)  
            self.update_versions()                     
        else:
            self.current_version = self.current_version['version_number']

        ## Create indexes. The operation is idempotent. Nothing will be done if the index is already there

        self.collection.create_index([('_first_processed_version',ASCENDING)])
        self.collection.create_index([('_last_processed_version',ASCENDING)])                
    
    def create_index(self, fields):
        if self.operation_mode == 'preprocess':
            self.collection_processed.create_index([(field,1) for field in fields]) ## in preprocess operation mode, queries are really executed over the processed collection
        elif self.operation_mode == 'rewrite':
            self.collection.create_index([(field,1) for field in fields])

    def update_versions(self):
        normalized = pd.json_normalize(self.collection_versions.find())
        self.versions_df = pd.DataFrame(normalized)

    def register_operation(self, OperationKey, SemanticOperationClass):
        self.semantic_operations[OperationKey] = SemanticOperationClass


    def insert_one(self, JsonString, ValidFromDate:datetime):
        """ Insert one documet in the collection.

        Args:
            JsonString(): string of the json representation of the document being inserted.
            valid_from_date(): date of when the register becomes valid. This is important in the treatment of semantic changes along time
        
        """               
        self.update_versions()

        versions = self.collection_versions.find({'version_valid_from':{'$lte' : ValidFromDate}}).sort('version_valid_from',DESCENDING)
        version = next(versions, None)                       
        
        self.__insert_one_by_version(JsonString, version['version_number'], ValidFromDate)                


    def __insert_one_by_version(self, JsonString, VersionNumber, ValidFromDate:datetime):        
        o = json.loads(JsonString)                
        o['_original_version']=VersionNumber                   
        o['_valid_from'] = ValidFromDate    

        for key, value in o.items():
            if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                o[key] = datetime.fromisoformat(value)                                    

        insertedDocument = self.collection.insert_one(o)        

        if self.operation_mode != 'preprocess':
            return

        p = json.loads(JsonString)        
        p['_original_id'] = insertedDocument.inserted_id
        p['_original_version'] = VersionNumber
        p['_valid_from'] = ValidFromDate
        p['_evoluted'] = False
        p['_evolution_list'] = []    
        

        ##Ideia é checar aqui se pode ter tido alterações semanticas para cada tipo,
        #dado que parametros sao diferentes. Ai splitar ja os processados e talvez ate processar 
        #na hora. Assim nao preciso ficar checando na consulta mais. 
        
        check_list = list()                
        insertion_list = list()                
        minVersion = float('-inf')
        maxVersion = float('inf')
        p['_min_version_number'] = minVersion
        p['_max_version_number'] = maxVersion
        check_list.append(p)        

        while len(check_list) > 0:
            document = check_list.pop()           

            for operationType in self.semantic_operations:
                if operationType in ['merging','translation','splitting','grouping','ungrouping']:
                    affected_versions = self.semantic_operations[operationType].check_if_affected(document)                                       
                    
                    if affected_versions != None:                                           
                        for v in affected_versions:
                            if v[2] == 'forward':
                                operation = self.versions_df.loc[(self.versions_df['next_version'] == v[1])]
                                new_document = self.semantic_operations[operation['next_operation.type'].values[0]].evolute_forward(document, operation)                                
                                new_document['_min_version_number'] = v[1]
                                document['_max_version_number'] = v[0]
                                new_document['_evoluted'] = True
                                new_document['_evolution_list'].append(v[0])
                                check_list.append(new_document)
                            else:
                                operation = self.versions_df.loc[(self.versions_df['previous_version'] == v[1])]
                                new_document = self.semantic_operations[operation['previous_operation.type'].values[0]].evolute_backward(document, operation)                                
                                new_document['_max_version_number'] = v[1]
                                document['_min_version_number'] = v[0]
                                new_document['_evoluted'] = True
                                new_document['_evolution_list'].append(v[0])
                                check_list.append(new_document)

            insertion_list.append(document)  ## If affected versions, document limits already updated when arrived here    
        

        self.collection_processed.insert_many(insertion_list)

        new_fields = list()
        for field in o:
            if not field.startswith('_'):                
                if field not in self.fields:
                    new_field = {'field_name': field, 'first_edit_version': minVersion, 'last_edit_version': maxVersion}                
                    new_fields.append(new_field)
                    self.fields[field] = (minVersion, maxVersion)

        if len(new_fields) > 0:
            self.collection_columns.insert_many(new_fields)
        

    def insert_many_by_csv(self, FilePath, ValidFromField, ValidFromDateFormat='%Y-%m-%d', Delimiter=','):
        """ Insert many recorDs in the collection using a csv file. 

        Args: 
            FilePath (): path to the csv file
            validValidFromField_from_field (): csv field which should be used to determine the date of when the register becomes valid. This is important in the treatment of semantic changes along time
            ValidFromDateFormat (): the expected format of the date contained in the valid_from_field values
            Delimiter (): delimiter used in the file. 

        """
        df = pd.read_csv(FilePath, delimiter=Delimiter)
        df[ValidFromField] = pd.to_datetime(df[ValidFromField], format=ValidFromDateFormat)

        self.insert_many_by_dataframe(df, ValidFromField)
    
    def insert_many_by_dataframe(self, dataframe, ValidFromField):        
        all_versions = self.collection_versions.find(projection=['version_valid_from','version_number'])
        dates = pd.DataFrame(all_versions).sort_values(by='version_valid_from')
        dates = pd.concat([
            dates,
            pd.DataFrame([{'version_valid_from': datetime(2200, 12, 31), 'version_number': float('inf')}])
        ], ignore_index=True)
        dates= dates.reset_index(drop=True)

        dates_1= dates.copy().reindex(index=np.roll(dates.index,-1))
        dates_1= dates_1.reset_index(drop=True)
        dates_guide = pd.concat([dates[['version_valid_from','version_number']],dates_1[['version_valid_from']]], axis=1).set_axis(['start','_original_version','end'], axis=1)

        r = dataframe.merge(dates_guide, how='cross')
        r[ValidFromField] = pd.to_datetime(r[ValidFromField])
        r['start'] = pd.to_datetime(r['start'])
        r['end'] = pd.to_datetime(r['end'])
        r['_valid_from'] = r[ValidFromField]
        r = r.loc[(r['start']<= r[ValidFromField]) & (r['end'] > r[ValidFromField])]
        r.rename(columns={'start':'version_valid_from'}, inplace=True)

        ##Preciso dar um jeito de passar o version number ja. Ja estou passando o valid from também. Para tentar processar tudo de uma vez só
        # r_2 = pd.merge(r, self.versions_df, on='version_valid_from')
        # r_2.rename(columns={'version_number':'_original_version'}, inplace=True)

        cols = list(dataframe.columns)
        cols.append('_valid_from')
        cols.append('_original_version')


        self.__insert_many_by_version(r[cols])


    def __insert_many_by_version(self, Group: pd.DataFrame):

        processed_group = Group.copy()              
        

        insertedDocuments = self.collection.insert_many(Group.to_dict('records'))         

        if self.operation_mode != 'preprocess':
            return     

        processed_group.insert(len(processed_group.columns),'_original_id', insertedDocuments.inserted_ids) 
        
        minVersion = float('-inf')
        maxVersion = float('inf') 
 
        processed_group['_evoluted'] = False                     
        processed_group['_min_version_number'] = minVersion
        processed_group['_max_version_number'] = maxVersion
        #processed_group['_evolution_list'] = []

        ### Verificação e processamento das alterações semanticas
        cols = list(processed_group.columns)
        recheck_group = processed_group.copy()

        i = 0

        while len(recheck_group) > 0:
            if i == 300:
                recheck_group.to_csv('recheck_300.csv', index=False)
                raise BaseException('Too many iterations. Checkpoint saved in recheck_300.csv')

            i+=1
            
            g = recheck_group[cols]
            recheck_group = pd.DataFrame()

            for operationType in self.semantic_operations:                
                affected_versions = self.semantic_operations[operationType].check_if_many_affected(g)

                j = 0
                if affected_versions != None:                                                               
                    j+=1
                    
                    for idx in range(len(affected_versions)):
                        v = affected_versions[idx]

                        versions_updated = False

                        if v[2] == 'backward':
                            altered = self.semantic_operations[operationType].evolute_many_backward(v[0], v[1])
                            altered['_max_version_number'] = altered['previous_version']
                            v[1]['_min_version_number'] = v[1]['version_number'] #matched records before semantic evolution
                            
                            recheck_group = pd.concat([recheck_group,altered, v[1]])   
                            alt_list = list(altered['_original_id'])                             
                            g=g.loc[~g['_original_id'].isin(alt_list)] 

                            if len(altered) >0:
                                versions_updated = True
                        else:
                            altered = self.semantic_operations[operationType].evolute_many_forward(v[0], v[1])
                            altered['_min_version_number'] = altered['next_version']
                            v[1]['_max_version_number'] = v[1]['version_number'] #matched records before semantic evolution
                            recheck_group = pd.concat([recheck_group,altered, v[1]])
                            alt_list = list(altered['_original_id'])                             
                            g=g.loc[~g['_original_id'].isin(alt_list)] 

                            if len(altered) >0:
                                versions_updated = True
                        
                        if versions_updated: ## If there has been any alteration, affected versions must be checked under new min_version_number and max_version_number, to avoid a loop of alterations due to unupdated versions
                            recheck_affected = pd.concat([g, altered[cols], (v[1])[cols]])
                            affected_versions = self.semantic_operations[operationType].check_if_many_affected(recheck_affected)
                        
            
            if len(g) > 0: # O que ta no g nao foi tocado por nenhuma alteração semantica e já pode ser inserido direto
                self.collection_processed.insert_many(g.to_dict('records'))
        
    
    def __process_query(self,QueryString):
        forward = self.__process_query_forward(QueryString)
        backward = self.__process_query_backward(QueryString)

        return {'$or':[forward, backward]}

#################################### QUERY REWRITING #######################################################################
    def __check_regex(self, query_dict, previous_fieldValue, next_fieldValue):
        query_dict = query_dict.copy()
        for key in list(query_dict.keys()):
            if '$regex' in key:
                expression = f'.*{query_dict[key]}.*'
                if re.match(expression, previous_fieldValue):                    
                    new_value = re.sub(expression, next_fieldValue, query_dict[key])
                    query_dict.pop(key)                    
                    query_dict['$in'] = [new_value]            
        return query_dict

    def __rewrite_queryterms_forward(self, QueryString, queryTerms):

        for field in QueryString.keys():  
            if field in self.logic_operators:
                queryTerms[field] = dict()
                if isinstance(QueryString[field], list):
                    for term in QueryString[field]:                        
                        self.__rewrite_queryterms_forward(term, queryTerms[field])
                else:
                    self.__rewrite_queryterms_forward(QueryString[field], queryTerms[field])                
                continue            

            if field not in queryTerms:
                queryTerms[field] = list()                

            ##Raw query must also be added in the final result
            queryTerms[field].append(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()

                has_evolution=False
                
                #Lets be sure we do not get in an infinite loop here                                
                if isinstance(fieldValue, tuple):                    
                    fieldValueRaw = fieldValue[0]                        
                    p_version_start = fieldValue[2]                
                    version_start = fieldValue[3]   
                    valueOrigin = fieldValue[4]             
                    fieldValueQ = fieldValueRaw                        
                    valueOriginOriginal = fieldValue[5]
                    fieldValueOriginal = fieldValue[6]

                    has_evolution=True

                    ## node in stack already represents a condition to rewrite
                    queryTerms[field].append((fieldValueRaw, p_version_start, version_start, valueOrigin, 'forward', valueOriginOriginal,fieldValueOriginal))                

                    while isinstance(fieldValueQ, dict):                        
                        keys = list(fieldValueQ.keys())
                        if isinstance(fieldValueQ[keys[0]], dict):
                            fieldValueQ = fieldValueQ[keys[0]]
                        elif keys[0][0] == '$':
                            fieldValueQ = fieldValueRaw
                            break
                        else:
                            fieldValueQ = fieldValueQ[keys[0]]                    

                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ, 'version_number':{'$gt': fieldValue[1]}}                    
                else:                    
                    fieldValueRaw = fieldValue
                    fieldValueQ = fieldValueRaw                                        

                    next_fieldValue = None
                    version_number = None
                    version_start = None
                    
                    while isinstance(fieldValueQ, dict):                        
                        keys = list(fieldValueQ.keys())
                        if isinstance(fieldValueQ[keys[0]], dict):
                            fieldValueQ = fieldValueQ[keys[0]]
                        elif keys[0][0] == '$':
                            fieldValueQ = fieldValueRaw
                            break
                        else:
                            fieldValueQ = fieldValueQ[keys[0]]                                        
                    
                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ}
                
                versions = self.collection_versions.count_documents(q)

                
                if(versions > 0): ##Existe alguma coisa a ser processada sobre este campo ainda                    
                    versions = self.collection_versions.find(q).sort('version_number')
                    ## Not all the changes in this field might match the queried values. Therefore, we cannot only use all "previous start date"
                    ## for any file in the field
                    

                    for version in versions:
                        operation_type = version['next_operation']['type']
                        
                        if not self.semantic_operations[operation_type].forward_processable:
                            continue

                        previous_fieldValue = version['next_operation']['from']
                        next_fieldValue = version['next_operation']['to']
                        version_number = version['version_number']
                        version_end = datetime(2300,1,1)
                        p_version_start = version['version_valid_from']

                        
                        ## Depending on the semantic operation, the next value can be a list (ungrouping) or a single value (translation and grouping)
                        if isinstance(next_fieldValue,list):
                            next_fieldValues = next_fieldValue
                        else:
                            next_fieldValues = [next_fieldValue]                        

                        for next_fieldValue in next_fieldValues:
                            ##The next_fieldValue will remain as a list or static value to be used in transformations after the query
                            ##while the next_fieldValue_processed will be the value to be used in the query itself
                            next_fieldValue_processed = next_fieldValue  
                            fieldValueRaw_copy = fieldValueRaw #Creating a copy in case regex applies to a list. Original value cannot be ovewritten

                            if isinstance(fieldValueRaw, dict):
                                fieldValueRaw_copy = self.__check_regex(fieldValueRaw, previous_fieldValue, next_fieldValue_processed)

                                next_fieldValue_processed = json.dumps(fieldValueRaw_copy).replace(str(previous_fieldValue), str(next_fieldValue_processed))
                                next_fieldValue_processed = json.loads(next_fieldValue_processed)

                            ##We need to check the next version after this to determine if the start of the version is the end of the previous version                        
                            next_next_version = self.collection_versions.find_one({'version_number':version['next_version']})
                            if next_next_version.get('next_operation',None) != None and next_next_version['next_operation']['from'] == next_fieldValue_processed:
                                version_end = next_next_version['next_version_valid_from']

                            #besides from the original value, this value also represents a record that was translated in the past 
                            # from the original query term. Therefore, it must be considered in the query                        
                            to_process.append((next_fieldValue_processed,version_number, p_version_start, version_end, fieldValueQ,previous_fieldValue, next_fieldValue)) 
                else:
                    if has_evolution == True:
                        #This was the last evolution on this field, from oldest to newest. Next version should be the last version
                        if len(queryTerms[field]) > 0 and isinstance(queryTerms[field][-1], tuple):
                            last_value = queryTerms[field].pop()                            
                            new_tuple = (last_value[0], last_value[1], datetime(2300,1,1), last_value[3], last_value[4], last_value[5], last_value[6])
                            queryTerms[field].append(new_tuple)

                

    def __rewrite_queryterms_backward(self, QueryString, queryTerms):

        for field in QueryString.keys():  
            if field in self.logic_operators:
                queryTerms[field] = dict()
                if isinstance(QueryString[field], list):
                    for term in QueryString[field]:                        
                        self.__rewrite_queryterms_backward(term, queryTerms[field])
                else:
                    self.__rewrite_queryterms_backward(QueryString[field], queryTerms[field])                
                continue            

            if field not in queryTerms:
                queryTerms[field] = list()                            

            ##Raw query must also be added in the final result
            queryTerms[field].append(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()

                has_evolution = False
                
                #Lets be sure we do not get in an infinite loop here                                
                if isinstance(fieldValue, tuple):                    
                    fieldValueRaw = fieldValue[0]                        
                    p_version_start = fieldValue[2]                
                    version_start = fieldValue[3] 
                    valueOrigin = fieldValue[4]
                    fieldValueQ = fieldValueRaw  
                    valueOriginOriginal = fieldValue[5]
                    fieldValueOriginal = fieldValue[6]

                    has_evolution=True

                    ## node in stack already represents a condition to rewrite
                    queryTerms[field].append((fieldValueRaw, p_version_start, version_start, valueOrigin, 'backward', valueOriginOriginal, fieldValueOriginal))

                    while isinstance(fieldValueQ, dict):                        
                        keys = list(fieldValueQ.keys())
                        if isinstance(fieldValueQ[keys[0]], dict):
                            fieldValueQ = fieldValueQ[keys[0]]
                        elif keys[0][0] == '$':
                            fieldValueQ = fieldValueRaw
                            break
                        else:
                            fieldValueQ = fieldValueQ[keys[0]]                    

                    q = {'previous_operation.field':field,'previous_operation.from':fieldValueQ, 'version_number':{'$lt': fieldValue[1]}}                    
                else:                    
                    fieldValueRaw = fieldValue
                    fieldValueQ = fieldValueRaw                    

                    previous_fieldValue = None
                    version_number = None
                    version_start = None
                    
                    while isinstance(fieldValueQ, dict):                        
                        keys = list(fieldValueQ.keys())
                        if isinstance(fieldValueQ[keys[0]], dict):
                            fieldValueQ = fieldValueQ[keys[0]]
                        elif keys[0][0] == '$':
                            fieldValueQ = fieldValueRaw
                            break
                        else:
                            fieldValueQ = fieldValueQ[keys[0]]                    
                    
                    q = {'previous_operation.field':field,'previous_operation.from':fieldValueQ}

                
                ## Now looking for the next nodes
                versions = self.collection_versions.count_documents(q)               
                
                if(versions > 0): ##Existe alguma coisa a ser processada sobre este campo ainda                    

                    versions = self.collection_versions.find(q).sort('version_number', DESCENDING)
                    for version in versions:
                        operation_type = version['previous_operation']['type']
                        
                        if not self.semantic_operations[operation_type].backward_processable:
                            continue

                        previous_from_fieldValue = version['previous_operation']['from']
                        previous_fieldValue = version['previous_operation']['to']
                        version_number = version['version_number']
                        version_start = version['version_valid_from']
                        p_version_start = version['previous_version_valid_from']

                        
                        ## Depending on the semantic operation, the previous value can be a list (ungrouping) or a single value (translation and grouping)
                        if isinstance(previous_fieldValue,list):
                            previous_fieldValues = previous_fieldValue
                        else:
                            previous_fieldValues = [previous_fieldValue]


                        for previous_fieldValue in previous_fieldValues:
                            ##The previous_fieldValue will remain as a list or static value to be used in transformations after the query
                            ##while the previous_fieldValue_processed will be the value to be used in the query itself
                            previous_fieldValue_processed = previous_fieldValue
                            fieldValueRaw_copy = fieldValueRaw #Creating a copy in case regex applies to a list. Original value cannot be ovewritten
                            if isinstance(fieldValueRaw, dict):
                                fieldValueRaw_copy = self.__check_regex(fieldValueRaw, previous_from_fieldValue, previous_fieldValue_processed)

                                previous_fieldValue_processed = json.dumps(fieldValueRaw_copy).replace(str(previous_from_fieldValue), str(previous_fieldValue_processed))
                                previous_fieldValue_processed = json.loads(previous_fieldValue_processed)

                            #besides from the original value, this value also represents a record that was translated in the past 
                            # from the original query term. Therefore, it must be considered in the query                        
                            to_process.append((previous_fieldValue_processed,version_number, p_version_start, version_start, fieldValueQ, previous_from_fieldValue, previous_fieldValue))
                    
                else:
                    if has_evolution == True:
                        #This was the last evolution on this field, from newest to oldest. Previous version should be the first version
                        if len(queryTerms[field]) > 0 and isinstance(queryTerms[field][-1], tuple):
                            last_value = queryTerms[field].pop()                            
                            new_tuple = (last_value[0], datetime(1700,1,1), last_value[2], last_value[3], last_value[4], last_value[5], last_value[6])
                            queryTerms[field].append(new_tuple)

    def __assemble_query(self, key, valueSet):        
               
        if key in self.logic_operators:
            items = []
            for subkey in valueSet:
                items.extend([self.__assemble_query(subkey, valueSet[subkey])])

            return (key, items)
        
        ors = []

        for value in valueSet:
            if isinstance(value,tuple):
                p_version_start = value[1]
                next_version_start = value[2]
                
                if p_version_start == None:
                    ors.append({'$and':[{key:value[0]},{'_valid_from':{'$lte':next_version_start}}]})
                elif next_version_start == None:
                    ors.append({'$and':[{key:value[0]},{'_valid_from':{'$gt':p_version_start}}]})
                else:
                    ors.append({'$and':[{key:value[0]},{'_valid_from':{'$gt':p_version_start}},{'_valid_from':{'$lte':next_version_start}}]})
            else:
                ors.append({key:value})

        return ors
    

    ## This function could be integrated with __assemble_query. But for the sake of readability, it has been separated
    def __prepare_transformation(self, key, valueSet):        
               
        rows = []
        if key in self.logic_operators:            
            for subkey in valueSet:
                rows.extend([self.__prepare_transformation(subkey, valueSet[subkey])])                  
        

        for value in valueSet:
            if isinstance(value,tuple):
                rows.append({
                    'start': value[1],
                    'end': value[2],
                    'field': key,
                    'from':value[5],
                    'to': value[6], #Using the original value of transformation
                    'direction': value[4]
                })               
            

        return pd.DataFrame.from_records(rows)
    
    def __transform_results_df(self, transformation_df):        
        
        transformation_df['s'] = transformation_df['from']
        transformation_df.loc[transformation_df['direction']=='backward','from'] = transformation_df.loc[transformation_df['direction']=='backward','to']
        transformation_df.loc[transformation_df['direction']=='backward','to'] = transformation_df.loc[transformation_df['direction']=='backward','s']
        transformation_df.drop(columns=['s'],inplace=True)
        
        return transformation_df
    
    def __execute_agregation(self, finalQuery, transformation_df):

        transformation_documents = transformation_df.to_dict('records')

        pipeline = [
            {
                "$match": finalQuery
            },
            {
                "$set": {
                    "processedFields": {
                        "$reduce": {
                            "input": transformation_documents,
                            "initialValue": "$$ROOT",
                            "in": {
                                "$let": {
                                    "vars": {
                                        "fieldValue": {
                                            "$arrayElemAt": [
                                                {
                                                    "$filter": {
                                                        "input": { "$objectToArray": "$$value" },
                                                        "as": "item",
                                                        "cond": { "$eq": ["$$item.k", "$$this.field"] }
                                                    }
                                                },
                                                0
                                            ]
                                        }
                                    },
                                    "in": {
                                        "$cond": {
                                            "if": {
                                                "$and": [
                                                    { "$eq": ["$$this.from", "$$fieldValue.v"] },
                                                    { "$gte": ["$$value._valid_from", "$$this.start"] },
                                                    { "$lte": ["$$value._valid_from", "$$this.end"] }
                                                ]
                                            },
                                            "then": {
                                                "$mergeObjects": [
                                                    "$$value",
                                                    { "$arrayToObject": [[{ "k": "$$this.field", "v": "$$this.to" }]] }
                                                ]
                                            },
                                            "else": "$$value"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },            
            {
                "$replaceRoot": { "newRoot": "$processedFields" }
            }
        ]
        
        results = self.collection.aggregate(pipeline)

        return results   


    def __rewrite_and_query(self, QueryString):
        queryTermsForward = {}        
        self.__rewrite_queryterms_forward(QueryString,queryTermsForward)

        queryTermsBackward = {}        
        self.__rewrite_queryterms_backward(QueryString,queryTermsBackward)       

        
        ##Merging the two dictionaries together
        for field in queryTermsBackward.keys():      
            if field in queryTermsForward:
                if isinstance(queryTermsForward[field], list):
                    if (queryTermsBackward[field], list):
                        queryTermsForward[field].extend(queryTermsBackward[field])
                    else:
                        queryTermsForward[field].append(queryTermsBackward[field])
                else:
                    queryTermsForward[field] = [queryTermsForward[field], queryTermsBackward[field]]


        #### assembling final query
        transformation_df = pd.DataFrame(columns=['start','end','field','from','to','direction'])
        ands = []
        for field in queryTermsForward.keys():           

            rewritten_structure = self.__assemble_query(field, queryTermsForward[field])
            rows_transformation = self.__prepare_transformation(field, queryTermsForward[field])
            transformation_df = pd.concat([transformation_df, rows_transformation])

            if isinstance(rewritten_structure, tuple):
                all_items = list()
                for rewritten_subquery in rewritten_structure[1]:                    
                    all_items.append({'$or':[rewritten_subquery]})
                
                ands.append({rewritten_structure[0]:all_items})
            else:
                ands.append({'$or' : rewritten_structure})

        finalQuery = {'$and' : ands}    

        #records = list(self.collection.find(finalQuery))
        transformation_df = self.__transform_results_df(transformation_df)

        records = self.__execute_agregation(finalQuery, transformation_df)
        
        return records
    
############################################################################################################################    

    def __process_query_forward(self,QueryString):
        queryTerms = {}        
        
        for field in QueryString.keys():             
            queryTerms[field] = list()            
            queryTerms[field].append(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()
                
                #Lets be sure we do not get in an infinite loop here
                if isinstance(fieldValue, tuple):                    
                    fieldValueQ = fieldValue[0]
                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ, 'version_number':{'$gt': fieldValue[1]}}                    
                else:                    
                    fieldValueQ = fieldValue
                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ}
                
                versions = self.collection_versions.count_documents(q)

                version_number = None
                version_id=None
                if(versions > 0):
                    versions = self.collection_versions.find(q).sort('version_number')
                    for version in versions:
                        operation_type = version['next_operation']['type']
                        
                        if not self.semantic_operations[operation_type].forward_processable:
                            continue

                        fieldValue = version['next_operation']['to']
                        version_number = version['version_number']
                        version_id = version['_id']                        

                        if isinstance(fieldValue, list):
                            for f in fieldValue:
                                to_process.append((f, version_number, version_id))
                        else:
                            to_process.append((fieldValue,version_number, version_id)) #besides from the original query, this value could also represent a record that were translated in the past from the original query term. Therefore, it must be considered in the query                        
                
                if version_number == None:                    
                    queryTerms[field].append(fieldValue) 
                else:
                    if isinstance(fieldValue, list):
                        for f in fieldValue:                            
                            queryTerms[field].append((f,version_number,version_id))     
                    else:                        
                        queryTerms[field].append((fieldValue,version_number, version_id))

                
        
        ands = []

        for field in queryTerms:
            ors = []

            for value in queryTerms[field]:
                if isinstance(value,tuple):
                    ors.append({'$and':[{field:value[0]},{'_evolution_list':ObjectId(value[2])}]})
                else:
                    ors.append({field:value})

            ands.append({'$or' : ors})

        finalQuery = {'$and' : ands}    
        
        return finalQuery

    def __process_query_backward(self,QueryString):
        queryTerms = {}        
        
        for field in QueryString.keys():             
            queryTerms[field] = list()
            queryTerms[field].append(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()

                #Lets be sure we do not get in an infinite loop here
                if isinstance(fieldValue, tuple):                    
                    fieldValueQ = fieldValue[0]
                    q = {'previous_operation.field':field,'previous_operation.from':fieldValueQ, 'version_number':{'$lt': fieldValue[1]}}                    
                else:                    
                    fieldValueQ = fieldValue
                    q = {'previous_operation.field':field,'previous_operation.from':fieldValueQ}
                
                versions = self.collection_versions.count_documents(q)
                version_number = None
                version_id=None
                if(versions > 0):
                    versions = self.collection_versions.find(q).sort('version_number', DESCENDING)
                    for version in versions:
                        operation_type = version['previous_operation']['type']
                        
                        if not self.semantic_operations[operation_type].backward_processable:
                            continue
                        
                        fieldValue = version['previous_operation']['to']
                        version_number = version['version_number']
                        version_id = version['_id']                        

                        if isinstance(fieldValue,list):
                            for f in fieldValue:
                                to_process.append((f,version_number,version_id))
                        else:
                            to_process.append((fieldValue,version_number, version_id)) #besides from the original query, this value could also represent a record that were translated in the past from the original query term. Therefore, it must be considered in the query                        
                
                if version_number == None:                    
                    queryTerms[field].append(fieldValue)
                else:
                    if isinstance(fieldValue, list):
                        for f in fieldValue:                            
                            queryTerms[field].append((f,version_number,version_id))     
                    else:                        
                        queryTerms[field].append((fieldValue,version_number, version_id)) 

                
        
        ands = []

        for field in queryTerms:
            ors = []

            for value in queryTerms[field]:
                if isinstance(value,tuple):
                    ors.append({'$and':[{field:value[0]},{'_evolution_list':ObjectId(value[2])}]})
                else:
                    ors.append({field:value})

            ands.append({'$or' : ors})

        finalQuery = {'$and' : ands}    
        
        return finalQuery



    def count_documents(self, QueryString):
        finalQuery = self.__process_query(QueryString)        
        return self.__query_specific(finalQuery, isCount=True)

    
    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def find_many(self, QueryString):
        """ Query the collection with the supplied queryString

        Args:
            queryString(): string of the query in json. The syntax is the same as the pymongo syntax.
        
        Returns: a cursor for the records returned by the query

        """

        if self.operation_mode == 'preprocess':            
            finalQuery = self.__process_query(QueryString)                          
            r = self.__query_specific(finalQuery)     
        else:
            r = self.__rewrite_and_query(QueryString)

        return r

    def __query_specific(self, Query, VersionNumber=None, isCount=False):
        #Eu preciso depois permitir que seja o tempo da versao, nao o numero
        #E tambem que dados possam ser inseridos com tempo anterior, e assumir a versao da época.

        if(VersionNumber == None):
            VersionNumber = self.current_version           
        
        ##obtaining version to be queried
        
        to_process = []
        to_process.append(Query) 
        
        
        while len(to_process) > 0:
            field = to_process.pop()

            if isinstance(field, dict): 
                if(len(field.keys()) > 1):
                    for f in field.keys():
                        to_process.append({field: field[f]})
                    continue           
                else:
                    key = list(field.keys())[0]
                    value = field[key]
                    
                    if not isinstance(value, str):
                        to_process.append(value)
                    
                    field = key #keep process going for this iteration
            
            elif isinstance(field, list):
                to_process.extend(field)
                continue

            if not isinstance(field,str) or field[0] == '$' or field[0]=='_': #pymongo operators like $and, $or, etc                
                continue                           

        Query['_min_version_number'] = {'$lte' : VersionNumber} ##Retornando registros traduzidos. 
        Query['_max_version_number'] = {'$gte' : VersionNumber} ##Retornando registros traduzidos. 

        if isCount:
            return self.collection_processed.count_documents(Query)
        else:
            return self.collection_processed.find(Query)

    def execute_operation(self, operationType, validFrom, args):                  
        operation = self.semantic_operations[operationType]
        operation.execute_operation(validFromDate=validFrom, args=args)

    def execute_many_operations_by_csv(self, filePath, operationTypeColumn, validFromColumn):
        operations = pd.read_csv(filePath, sep=';') 
        operations = operations.sort_values(by=validFromColumn)

        i = 0

        for index, row in operations.loc[operations['type']=='translation'].iterrows():
            i = i + 1
            print('Executing translation ' + str(i))
            self.execute_operation('translation', datetime.strptime(row['valid_from'], '%Y-%m-%d'), {'fieldName':row['field'], 'oldValue':row['from'], 'newValue':row['to']})            

        # Support both new and old operation names for merging/grouping
        for group in operations.loc[operations['type'].isin(['merging', 'grouping'])].groupby(['to','valid_from','field']):
            i = i + 1
            operation_name = 'merging' if 'merging' in operations['type'].values else 'grouping'
            print('Executing merging ' + str(i))
            self.execute_operation('merging', datetime.strptime(group[0][1], '%Y-%m-%d'), {'fieldName':group[0][2], 'oldValues':list(group[1]['from'].values), 'newValue':group[0][0]})            

        # Support both new and old operation names for splitting/ungrouping
        for group in operations.loc[operations['type'].isin(['splitting', 'ungrouping'])].groupby(['from','valid_from','field']):
            i = i + 1
            operation_name = 'splitting' if 'splitting' in operations['type'].values else 'ungrouping'
            print('Executing splitting ' + str(i))
            self.execute_operation('splitting', datetime.strptime(group[0][1], '%Y-%m-%d'), {'fieldName':group[0][2], 'oldValue':group[0][0], 'newValues':list(group[1]['to'].values)})            


    def check_if_operation_affected_forward(self, fieldName, newValue, version_number):
        versions = self.collection_versions.find({'$and': [{'next_operation.field' : fieldName},                                                                      
                                                                      {'next_operation.from' : newValue}, #We are checking if the informed evolution here affected any pre-existing evolutions so as to reprocess them. Gonna do it in both directions
                                                                      {'next_version': {'$gt': version_number}}
                                                        ]}).sort('next_version_valid_from',ASCENDING)

        for version_change in versions:           
            
            # A previous evolution has been hit by this new evolution. We need to reprocess it.
            semantic_evolution = self.semantic_operations[version_change['next_operation']['type']]

            if semantic_evolution.forward_processable:
                semantic_evolution.reapply_operation_forward(version_change)            

            ##Recheck
            self.check_if_operation_affected_forward(version_change['next_operation']['field'], version_change['next_operation']['to'],version_change['next_version'])#Recheck if affected any other evolution

    def check_if_operation_affected_backward(self, fieldName, newValue,version_number):
        versions = self.collection.collection_versions.find({'$and': [{'previous_operation.field' : fieldName},                                                    
                                                    {'previous_operation.from' : newValue}, #We are checking if the informed evolution here affected any pre-existing evolutions so as to reprocess them. 
                                                    {'previous_version_number': {'$lt': version_number}}
                                                    ]}).sort('previous_version_valid_from',DESCENDING)

        for version_change in versions:         

            # A previous evolution has been hit by this new evolution. We need to reprocess it.
            semantic_evolution = self.semantic_operations[version_change['previous_operation']['type']]
            if semantic_evolution.backward_processable:
                semantic_evolution.reapply_operation_backward(version_change)
            
            self.check_if_operation_affected_backward(version_change['previous_operation']['field'], version_change['previous_operation']['to'],version_change['previous_version'])#Recheck if affected any other evolution
    


    def pretty_print(self, recordsCursor):
        """ Pretty print the records in the console. Semantic changes will be presented in the format "CurrentValue (originally: OriginalValue)"

        Args:
            recordsCursor(): the cursor for the records, usually obtained through the find functions
        
        """        
        fieldLengths = {}
             
        records = list(recordsCursor)
        
        for record in records:            

            if record['_evoluted'] == True:
                originalRecord = self.collection.find_one({'_id' : record['_original_id']})

            for field in record.keys():               

                if not field.startswith('_'):                    
                    if(field not in fieldLengths):
                        fieldLengths[field] = len(field) #Header is the first set length

                    if record['_evoluted'] == True:
                        processedValue = record[field]
                        originalValue = originalRecord[field]

                        if processedValue != originalValue:
                            record[field] = f'{processedValue} (originaly: {originalValue})'

                    if(fieldLengths[field] < len(str(record[field]))):
                        fieldLengths[field] = len(str(record[field]))

        lineLength = 0
        for field in fieldLengths.keys():
            p = '|' + field + ' '*(fieldLengths[field] - len(field))
            print(p, end='')
            lineLength = lineLength + len(p)
        
        print('|')                   
        print ('-'*(lineLength+1))
        
        for record in records:            
            for field in record.keys():  #calculate number of spaces so as fields have the same length in all records
                if not field.startswith('_'):
                    print('|' + str(record[field]) + ' '*(fieldLengths[field] - len(str(record[field]))), end='')
            print('|')