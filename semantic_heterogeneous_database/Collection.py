import time
import pandas as pd
import numpy as np
from pymongo import MongoClient, ASCENDING,DESCENDING
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo import ReadPreference
from datetime import datetime
from .exceptions import MellowDBError
from . import sharding
import json
import csv
import re

class Collection:
    #: Records lacking the shard key field are split through the driver, this many at a time.
    _CLIENT_SPLIT_BATCH = 1000

    def __init__(self, DatabaseName, CollectionName,
             mongo_uri='mongodb://localhost:27017',
             operation_mode='preprocess',
             write_concern='majority',
             read_uri=None,
             read_mode='split',
             shard_key=None):

        if not isinstance(operation_mode, str) or operation_mode not in ['preprocess','rewrite']:
            raise MellowDBError('Operation Mode not recognized')
        
        self.operation_mode = operation_mode        
        self.operations = {}
        self.logic_operators = ['$or','$and','$xor','$not','$nor']    

        self.client = MongoClient(mongo_uri)
        self._write_concern = WriteConcern(w=write_concern)
        self._is_replica_set = bool(
            self.client.admin.command('hello').get('setName')
        )

        self.database_name = DatabaseName
        self.db = self.client[DatabaseName]

        existing_collections = self.db.list_collection_names()
        if operation_mode == 'rewrite':
            if (CollectionName + '_processed') in existing_collections:
                raise MellowDBError('Previous preprocessed collection already exists.')            
        else:
            if (CollectionName + '_processed') not in existing_collections and CollectionName in existing_collections:
                raise MellowDBError('Previous rewrite collection already exists.')            


        self.collection_name = CollectionName
        self.collection = self.db[CollectionName]
        self.collection_versions = self.db[CollectionName + '_versions']

        # Configure where the heavy record reads are served from. Defaults to the
        # primary (same client). A separate read_uri routes record reads to a chosen node.
        self._configure_read_source(read_uri, read_mode)

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

        ## Sharding is applied here, with the collections still empty, so MongoDB
        ## builds the shard key index itself. _versions and _columns are left
        ## unsharded on purpose: they are tiny and have two competing access
        ## patterns (next_operation.* and previous_operation.*) that no single
        ## shard key serves, so they stay whole on the database's primary shard.
        self._is_sharded = sharding.is_mongos(self.client)
        self._shard_key = sharding.ensure(self.db, self.collection_name, shard_key)
        self._processed_shard_key = None
        if operation_mode == 'preprocess':
            self._processed_shard_key = sharding.ensure(
                self.db, self.collection_name + '_processed', shard_key)

        self.current_version = self._versions_r.find_one({"current_version":1})
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
            self.collection_versions.with_options(
                write_concern=WriteConcern(w='majority')
            ).insert_one(first_version)
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
        normalized = pd.json_normalize(self._versions_r.find())
        self.versions_df = pd.DataFrame(normalized)

    @property
    def _col_w(self):
        """Raw collection with user-configured write concern."""
        return self.collection.with_options(write_concern=self._write_concern)

    @property
    def _col_processed_w(self):
        """Processed collection with user-configured write concern."""
        return self.collection_processed.with_options(write_concern=self._write_concern)

    @property
    def _col_columns_w(self):
        """Columns metadata collection with user-configured write concern."""
        return self.collection_columns.with_options(write_concern=self._write_concern)

    @property
    def _col_versions_w(self):
        """Versions collection with majority write concern. Used by operation classes for version chain writes."""
        return self.collection_versions.with_options(
            write_concern=WriteConcern(w='majority')
        )

    @property
    def _versions_r(self):
        """Read handle for the version chain.

        split (default): always from the PRIMARY with majority concern on replica
        sets — the version chain must be fresh so query translation stays correct.
        single_source: from the chosen read node (accepts staleness on purpose).
        """
        if self._read_mode == 'single_source' and self._read_client is not self.client:
            return self._read_versions
        if self._is_replica_set:
            return self.collection_versions.with_options(
                read_preference=ReadPreference.PRIMARY,
                read_concern=ReadConcern('majority')
            )
        return self.collection_versions

    def _configure_read_source(self, read_uri=None, read_mode='split'):
        """(Re)point where record reads are served from.

        read_uri=None      -> record reads use the main (primary) connection (default).
        read_uri='mongodb://host:port/?directConnection=true' -> record reads come
                              from exactly that node, independent of read preference.
        read_mode='split'  -> version chain stays on the primary; only records move.
        read_mode='single_source' -> version chain reads from the read node too.
        """
        if read_mode not in ('split', 'single_source'):
            raise MellowDBError("read_mode must be 'split' or 'single_source'")

        # Close a previous dedicated read client, but never the main client.
        previous = getattr(self, '_read_client', None)
        if previous is not None and previous is not self.client:
            previous.close()

        self._read_uri = read_uri
        self._read_mode = read_mode
        if read_uri is None:
            self._read_client = self.client
            self._read_db = self.db
        else:
            self._read_client = MongoClient(read_uri)
            self._read_db = self._read_client[self.database_name]

        self._collection_read = self._read_db[self.collection_name]
        self._processed_read = self._read_db[self.collection_name + '_processed']
        self._read_versions = self._read_db[self.collection_name + '_versions']

    def set_read_source(self, read_uri=None, read_mode='split'):
        """Switch the record-read node/mode at runtime without rebuilding the collection."""
        self._configure_read_source(read_uri, read_mode)

    def register_operation(self, OperationKey, SemanticOperationClass):
        self.semantic_operations[OperationKey] = SemanticOperationClass

    def split_processed_records(self, FieldName, ValueFilter, Version, EvolvedValue, Direction, EvolutionDate):
        """ Apply one semantic evolution to the processed records it covers. This is the
        core materialization step shared by all semantic operations and their reapplications.

        A forward evolution covers records valid before its date and a backward one the rest
        (paper, Definitions 3.3 and 3.4). It covers every row of such a record on the evolved
        side of Version: a row crossing Version is cut in two, and a row already entirely on
        that side, because an earlier operation cut it, is replaced. Only for an operation at
        the end of the chain: one registered earlier in the chain goes through
        rematerialize_records, since it can invalidate rows later operations derived.

        Args:
            FieldName(): field affected by the semantic operation
            ValueFilter(): value (or pymongo filter like {'$in': [...]}) the records carry
            Version(): version number where the evolution takes effect
            EvolvedValue(): value FieldName receives on the evolved side
            Direction(): 'forward' = evolved side is [Version, _max); 'backward' = [_min, Version)
            EvolutionDate(): date of the semantic operation

        """
        if Direction == 'forward':
            eligible = {'$lt': EvolutionDate}
            reaches = {'_max_version_number': {'$gt': Version}}
            whole = {'_min_version_number': {'$gte': Version}}
            evolved_range = {'_min_version_number': {'$max': ['$_min_version_number', Version]}}
            original_range = {'_max_version_number': Version}
        else:
            eligible = {'$gte': EvolutionDate}
            reaches = {'_min_version_number': {'$lt': Version}}
            whole = {'_max_version_number': {'$lte': Version}}
            evolved_range = {'_max_version_number': {'$min': ['$_max_version_number', Version]}}
            original_range = {'_min_version_number': Version}

        match = {'$and': [
            {'_valid_from': eligible},
            reaches,
            {FieldName: ValueFilter},
            {FieldName: {'$ne': EvolvedValue}}
        ]}

        evolved_set = {
            **evolved_range,
            FieldName: {'$literal': EvolvedValue},
            '_evoluted': True,
            ## The value this record was stored with, which query terms are expressed in
            f'_raw.{FieldName}': {'$ifNull': [f'$_raw.{FieldName}', f'${FieldName}']},
        }

        ## Copy-and-evolve in one server-side pass. Unlike $out, $merge can target a
        ## sharded collection and can target the collection being read, so there is no
        ## temporary collection to funnel every affected record through the primary
        ## shard. $unset of _id makes each copy a fresh insert, so whenMatched never
        ## fires; the $ne clause keeps the scan from ever seeing those inserts.
        ## No `on` is specified on purpose: the default needs no unique index.
        def evolved_copies(filter_):
            return [{'$match': filter_}, {'$unset': '_id'}, {'$set': evolved_set}]

        merge_back = {'$merge': {'into': self.collection_processed.name, 'whenMatched': 'fail'}}

        key = self._processed_shard_key
        if key is None:
            self._col_processed_w.aggregate(evolved_copies(match) + [merge_back])
        else:
            ## On a sharded collection $merge refuses any record missing the shard key
            ## field (Location51132), and only after the other shards have written their
            ## copies. Inserting such a record is accepted, so the records without the
            ## field - all under null, on one chunk - go through the same pipeline and
            ## are written back by the driver instead.
            self._col_processed_w.aggregate(
                evolved_copies({'$and': [match, {key.field: {'$ne': None}}]}) + [merge_back])
            batch = []
            for copy in self._col_processed_w.aggregate(
                    evolved_copies({'$and': [match, {key.field: None}]})):
                batch.append(copy)
                if len(batch) == self._CLIENT_SPLIT_BATCH:
                    self._col_processed_w.insert_many(batch)
                    batch = []
            if batch:
                self._col_processed_w.insert_many(batch)

        ## Deleted rather than updated in place: the evolved field may be the shard key.
        self._col_processed_w.delete_many({'$and': [match, whole]})
        self._col_processed_w.update_many(match, {'$set': original_range})


    def insert_one(self, JsonString, ValidFromDate:datetime):
        """ Insert one documet in the collection.

        Args:
            JsonString(): string of the json representation of the document being inserted.
            valid_from_date(): date of when the register becomes valid. This is important in the treatment of semantic changes along time
        
        """               
        versions = self.__version_chain()
        version_number = versions[self.__version_position(versions, ValidFromDate)]['version_number']
        self.__insert_one_by_version(JsonString, version_number, ValidFromDate, versions)


    def __version_chain(self):
        """Every version, in chain order, which is also chronological order."""
        return list(self._versions_r.find().sort('version_number', ASCENDING))

    @staticmethod
    def __version_position(versions, date):
        """Where in the chain the semantic version a record valid at `date` was born in sits."""
        return sum(1 for version in versions if version['version_valid_from'] <= date) - 1

    def __materialized_rows(self, record, versions):
        """Every processed row of one record: its original row, then a step along the chain
        each time an operation changes it, forward and backward (paper, Algorithm 3).

        Walking the chain in order is the point. The value a record carries when it reaches
        an operation decides whether that operation applies, so operations cannot be
        considered independently of one another.
        """
        position = self.__version_position(versions, record['_valid_from'])

        def evolved_from(row, field, value, lower, upper):
            return {**row, field: value, '_evoluted': True,
                    '_raw': {field: record[field], **row['_raw']},
                    '_min_version_number': lower, '_max_version_number': upper}

        def affects(operation, row):
            sources = operation['from'] if isinstance(operation['from'], list) else [operation['from']]
            return operation['field'] in row and row[operation['field']] in sources

        forward = dict(record)
        rows = []
        lower = float('-inf')
        for index in range(position, len(versions) - 1):
            operation = versions[index]['next_operation']
            if (self.semantic_operations[operation['type']].forward_reapplicable
                    and affects(operation, forward)):
                boundary = versions[index + 1]['version_number']
                rows.append({**forward, '_min_version_number': lower, '_max_version_number': boundary})
                forward = evolved_from(forward, operation['field'], operation['to'], boundary, float('inf'))
                lower = boundary
        rows.append({**forward, '_min_version_number': lower, '_max_version_number': float('inf')})

        backward = rows[0]
        for index in range(position, 0, -1):
            operation = versions[index]['previous_operation']
            if (self.semantic_operations[operation['type']].backward_reapplicable
                    and affects(operation, backward)):
                boundary = versions[index]['version_number']
                backward['_min_version_number'] = boundary
                backward = evolved_from(backward, operation['field'], operation['to'],
                                        float('-inf'), boundary)
                rows.append(backward)

        return rows

    def rematerialize_records(self, field, values):
        """Derive again every row of the records an operation registered earlier in the chain
        can reach. Such an operation changes what later operations see, so rows already derived
        from them may no longer exist in the new chain; only a replay from the stored record
        can tell.
        """
        versions = self.__version_chain()
        operations = [version['next_operation'] for version in versions
                      if version.get('next_operation') and version['next_operation']['field'] == field]

        def values_of(operation):
            return [*(operation['from'] if isinstance(operation['from'], list) else [operation['from']]),
                    *(operation['to'] if isinstance(operation['to'], list) else [operation['to']])]

        ## Everything connected through the chain: a record can arrive at an affected value
        ## from any of these, and a later row can start from one.
        affected = set(values)
        grown = True
        while grown:
            grown = False
            for operation in operations:
                involved = set(values_of(operation))
                if involved & affected and not involved <= affected:
                    affected |= involved
                    grown = True

        batch = []
        for stored in self.collection.find({field: {'$in': list(affected)}}):
            batch.append(stored)
            if len(batch) == self._CLIENT_SPLIT_BATCH:
                self.__replace_rows(batch, versions)
                batch = []
        if batch:
            self.__replace_rows(batch, versions)

    def __replace_rows(self, stored_records, versions):
        rows = []
        for stored in stored_records:
            record = {key: value for key, value in stored.items() if key != '_id'}
            record.update(_original_id=stored['_id'], _evoluted=False, _raw={},
                          _original_version=versions[self.__version_position(versions, stored['_valid_from'])]['version_number'])
            rows.extend(self.__materialized_rows(record, versions))
        self._col_processed_w.delete_many({'_original_id': {'$in': [stored['_id'] for stored in stored_records]}})
        self._col_processed_w.insert_many(rows)

    def __insert_one_by_version(self, JsonString, VersionNumber, ValidFromDate:datetime, versions):
        o = json.loads(JsonString)
        o['_original_version']=VersionNumber
        o['_valid_from'] = ValidFromDate

        for key, value in o.items():
            if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                o[key] = datetime.fromisoformat(value)

        insertedDocument = self._col_w.insert_one(o)

        if self.operation_mode != 'preprocess':
            return

        p = json.loads(JsonString)
        p['_original_id'] = insertedDocument.inserted_id
        p['_original_version'] = VersionNumber
        p['_valid_from'] = ValidFromDate
        p['_evoluted'] = False
        p['_raw'] = {}

        self._col_processed_w.insert_many(self.__materialized_rows(p, versions))

        minVersion = float('-inf')
        maxVersion = float('inf')
        new_fields = list()
        for field in o:
            if not field.startswith('_'):
                if field not in self.fields:
                    new_field = {'field_name': field, 'first_edit_version': minVersion, 'last_edit_version': maxVersion}
                    new_fields.append(new_field)
                    self.fields[field] = (minVersion, maxVersion)

        if len(new_fields) > 0:
            self._col_columns_w.insert_many(new_fields)


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
        all_versions = self._versions_r.find(projection=['version_valid_from','version_number'])
        dates = pd.DataFrame(all_versions).sort_values(by=['version_valid_from','version_number'])
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

        cols = list(dataframe.columns)
        cols.append('_valid_from')
        cols.append('_original_version')


        self.__insert_many_by_version(r[cols])


    def __insert_many_by_version(self, Group: pd.DataFrame):
        insertedDocuments = self._col_w.insert_many(Group.to_dict('records'))

        if self.operation_mode != 'preprocess':
            return

        processed_group = Group.copy()
        processed_group['_original_id'] = insertedDocuments.inserted_ids
        processed_group['_evoluted'] = False
        processed_group['_raw'] = [{} for _ in range(len(processed_group))]
        records = processed_group.to_dict('records')

        versions = self.__version_chain()
        if len(versions) == 1:
            for record in records:
                record['_min_version_number'] = float('-inf')
                record['_max_version_number'] = float('inf')
            self._col_processed_w.insert_many(records)
            return

        rows = []
        for record in records:
            rows.extend(self.__materialized_rows(record, versions))
        self._col_processed_w.insert_many(rows)

    def __evolution_chain(self, fields=None):
        """The registered semantic operations in chain order, read fresh for each query."""
        query = {'next_operation': {'$ne': None}}
        if fields is not None:
            query['next_operation.field'] = {'$in': list(fields)}

        chain = []
        for version in self._versions_r.find(query).sort('version_number', ASCENDING):
            operation = version['next_operation']
            semantic = self.semantic_operations[operation['type']]
            chain.append({
                'field': operation['field'],
                'sources': operation['from'] if isinstance(operation['from'], list) else [operation['from']],
                'targets': operation['to'] if isinstance(operation['to'], list) else [operation['to']],
                'date': version['next_version_valid_from'],
                'forward': semantic.forward_processable,
                'backward': semantic.backward_processable,
                'materialized': semantic.forward_reapplicable,
            })
        return chain

    @staticmethod
    def __query_terms(field, value, chain):
        """What a query for `value` means over the stored records, shared by both strategies.

        A set of (stored value, start, end): the records storing that value and valid in
        [start, end), None meaning unbounded (paper, Equation 8). Each hop on a value lasts
        until the next operation that moves that value on.
        """
        operations = [operation for operation in chain if operation['field'] == field]
        terms = {(value, None, None)}

        def forward(term, after):
            for position in range(after + 1, len(operations)):
                operation = operations[position]
                if operation['forward'] and term in operation['sources']:
                    for target in operation['targets']:
                        end = next((later['date'] for later in operations[position + 1:]
                                    if target in later['sources']), None)
                        terms.add((target, operation['date'], end))
                        forward(target, position)

        def backward(term, before):
            for position in range(before - 1, -1, -1):
                operation = operations[position]
                if operation['backward'] and term in operation['targets']:
                    for source in operation['sources']:
                        start = next((earlier['date'] for earlier in reversed(operations[:position])
                                      if source in earlier['targets']), None)
                        terms.add((source, start, operation['date']))
                        backward(source, position)

        forward(value, -1)
        backward(value, len(operations))
        return terms

    @staticmethod
    def __reachable_values(field, value, chain):
        """Every value a record storing `value` can carry at the latest semantic version."""
        reachable = {value}
        for operation in chain:
            if (operation['field'] == field and operation['materialized']
                    and reachable.intersection(operation['sources'])):
                reachable.update(operation['targets'])
        return reachable

    @staticmethod
    def __valid_in(start, end):
        bounds = {}
        if start is not None:
            bounds['$gte'] = start
        if end is not None:
            bounds['$lt'] = end
        return {'_valid_from': bounds} if bounds else {}

    @staticmethod
    def __any_of(clauses):
        return clauses[0] if len(clauses) == 1 else {'$or': clauses}

    @staticmethod
    def __expands(field, value):
        """Only equality on a plain value has a meaning across evolutions."""
        return not field.startswith('$') and not isinstance(value, (dict, list))

    def __processed_term(self, field, term, start, end, chain):
        """A stored-value term as a filter on the rows of the latest semantic version.

        Filtering on the evolved value as well as on _raw is what keeps the query
        targetable: an index or a shard key on the field still applies.
        """
        raw = f'_raw.{field}'
        reachable = self.__reachable_values(field, term, chain)
        if len(reachable) > 1:
            clause = {field: {'$in': list(reachable)},
                      '$or': [{raw: term}, {field: term, raw: None}]}
        elif any(operation['field'] == field and operation['materialized']
                 and term in operation['targets'] for operation in chain):
            clause = {field: term, raw: None}
        else:
            clause = {field: term}
        clause.update(self.__valid_in(start, end))
        return clause

    def __process_query(self,QueryString):
        expanded = [field for field, value in QueryString.items() if self.__expands(field, value)]
        chain = self.__evolution_chain(expanded) if expanded else []

        clauses = []
        for field, value in QueryString.items():
            if field not in expanded:
                clauses.append({field: value})
                continue
            clauses.append(self.__any_of([
                self.__processed_term(field, term, start, end, chain)
                for term, start, end in self.__query_terms(field, value, chain)]))

        return {'$and': clauses} if clauses else {}

    def __rewrite_values(self, QueryString):
        """Equality queries in rewrite mode: the same terms as preprocess, on stored values,
        with every record then projected to the latest semantic version (paper, Algorithm 2)."""
        chain = self.__evolution_chain()

        clauses = []
        reachable = {}
        for field, value in QueryString.items():
            terms = self.__query_terms(field, value, chain)
            clauses.append(self.__any_of([{field: term, **self.__valid_in(start, end)}
                                          for term, start, end in terms]))
            reachable[field] = set().union(*(self.__reachable_values(field, term, chain)
                                             for term, _, _ in terms))

        ## Only what can change a returned value: the whole chain on the other fields, and
        ## on a queried field only the operations its matching values can go through.
        evolutions = [
            {'field': operation['field'], 'from': source,
             'to': operation['targets'][0], 'date': operation['date']}
            for operation in chain if operation['materialized']
            for source in operation['sources']
            if operation['field'] not in reachable or source in reachable[operation['field']]]

        pipeline = [{'$match': {'$and': clauses} if clauses else {}}]
        if evolutions:
            pipeline.extend(self.__latest_version_projection(evolutions))
        return self._collection_read.aggregate(pipeline)

    @staticmethod
    def __latest_version_projection(evolutions):
        return [
            {'$replaceRoot': {'newRoot': {'$reduce': {
                'input': {'$literal': evolutions},
                'initialValue': '$$ROOT',
                'in': {'$let': {
                    'vars': {'current': {'$arrayElemAt': [
                        {'$filter': {'input': {'$objectToArray': '$$value'}, 'as': 'item',
                                     'cond': {'$eq': ['$$item.k', '$$this.field']}}}, 0]}},
                    'in': {'$cond': {
                        'if': {'$and': [{'$eq': ['$$this.from', '$$current.v']},
                                        {'$lt': ['$$value._valid_from', '$$this.date']}]},
                        'then': {'$mergeObjects': [
                            '$$value',
                            {'$arrayToObject': [[{'k': '$$this.field', 'v': '$$this.to'}]]}]},
                        'else': '$$value'}}}}}}}},
        ]

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
                
                versions = self._versions_r.count_documents(q)


                if(versions > 0): ##Existe alguma coisa a ser processada sobre este campo ainda
                    versions = self._versions_r.find(q).sort('version_number')
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
                            next_next_version = self._versions_r.find_one({'version_number':version['next_version']})
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
                versions = self._versions_r.count_documents(q)

                if(versions > 0): ##Existe alguma coisa a ser processada sobre este campo ainda

                    versions = self._versions_r.find(q).sort('version_number', DESCENDING)
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
        
        results = self._collection_read.aggregate(pipeline)

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
        elif all(self.__expands(field, value) for field, value in QueryString.items()):
            r = self.__rewrite_values(QueryString)
        else:
            r = self.__rewrite_and_query(QueryString)

        return r

    def __query_specific(self, Query, VersionNumber=None, isCount=False):
        #Eu preciso depois permitir que seja o tempo da versao, nao o numero
        #E tambem que dados possam ser inseridos com tempo anterior, e assumir a versao da época.

        if(VersionNumber == None):
            VersionNumber = self.current_version

        Query['_min_version_number'] = {'$lte' : VersionNumber} ##Retornando registros traduzidos.
        Query['_max_version_number'] = {'$gt' : VersionNumber} ##Retornando registros traduzidos. 

        if isCount:
            return self._processed_read.count_documents(Query)
        else:
            return self._processed_read.find(Query)

    def execute_operation(self, operationType, validFrom, args):                  
        operation = self.semantic_operations[operationType]
        operation.execute_operation(validFromDate=validFrom, args=args)

    def execute_many_operations_by_csv(self, filePath, operationTypeColumn, validFromColumn):
        operations = pd.read_csv(filePath, sep=';') 
        operations = operations.sort_values(by=validFromColumn)

        pending = []

        for index, row in operations.loc[operations['type']=='translation'].iterrows():
            pending.append(('translation', row['valid_from'], {'fieldName':row['field'], 'oldValue':row['from'], 'newValue':row['to']}))

        # Support both new and old operation names for merging/grouping
        for group in operations.loc[operations['type'].isin(['merging', 'grouping'])].groupby(['to','valid_from','field']):
            pending.append(('merging', group[0][1], {'fieldName':group[0][2], 'oldValues':list(group[1]['from'].values), 'newValue':group[0][0]}))

        # Support both new and old operation names for splitting/ungrouping
        for group in operations.loc[operations['type'].isin(['splitting', 'ungrouping'])].groupby(['from','valid_from','field']):
            pending.append(('splitting', group[0][1], {'fieldName':group[0][2], 'oldValue':group[0][0], 'newValues':list(group[1]['to'].values)}))

        ## Chronological across the three types, so every operation lands at the end of the
        ## chain and nothing has to be derived again. The sort is stable: same-date operations
        ## keep the order above, which is the order they compose in.
        pending.sort(key=lambda operation: operation[1])
        for i, (operation_type, valid_from, arguments) in enumerate(pending, start=1):
            print(f'Executing {operation_type} {i}')
            self.execute_operation(operation_type, datetime.strptime(valid_from, '%Y-%m-%d'), arguments)


    def check_if_operation_affected_backward(self, fieldName, newValue,version_number):
        versions = self._versions_r.find({'$and': [{'previous_operation.field' : fieldName},
                                                    {'previous_operation.from' : newValue}, #We are checking if the informed evolution here affected any pre-existing evolutions so as to reprocess them.
                                                    {'previous_version': {'$lt': version_number}}
                                                    ]}).sort('previous_version_valid_from',DESCENDING)

        for version_change in versions:

            # A previous evolution has been hit by this new evolution. We need to reprocess it.
            semantic_evolution = self.semantic_operations[version_change['previous_operation']['type']]
            ## backward_reapplicable gates record reprocessing; backward_processable gates query expansion and runs in the opposite direction for grouping/ungrouping
            if semantic_evolution.backward_reapplicable:
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
                originalRecord = self._collection_read.find_one({'_id' : record['_original_id']})

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