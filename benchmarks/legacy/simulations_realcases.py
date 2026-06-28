import argparse
import os
import shutil
import uuid
import time
import json
import random
import math
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from database_generator import DatabaseGenerator
from semantic_heterogeneous_database import BasicCollection
import re
pd.options.mode.chained_assignment = None  # default='warn'


class Comparator:
    def __init__(self, host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, number_of_operations, percent_of_heterogeneous_queries, percent_of_insertions, execution_try, output_file, indexes, query_source):
        self.operation_mode = operation_mode
        self.method = method
        self.dbname = dbname
        self.collectionname = collectionname
        self.source_folder = source_folder
        self.date_columns = date_columns
        self.csv_destination = csv_destination
        self.operations_file = operations_file
        self.number_of_operations = number_of_operations
        self.percent_of_heterogeneous_queries = percent_of_heterogeneous_queries
        self.percent_of_insertions = percent_of_insertions
        self.execution_try = execution_try
        self.host = host
        self.collection = BasicCollection(self.dbname, self.collectionname, self.host, self.operation_mode)
        self.output_file = output_file
        self.indexes = indexes or []  # Default to an empty list if None
        self.query_source = query_source

        os.makedirs(csv_destination, exist_ok=True)
        os.makedirs(query_source, exist_ok=True)

    def __create_indexes(self):
        if self.indexes:
            for index in self.indexes:
                self.collection.create_index(index)
        

    def __insert_first(self):            
        start = time.time()
        
        self.__create_indexes()
        self.collection.insert_many_by_csv(self.source_folder, self.date_columns)        
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')        
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }

        with open(f'{self.csv_destination}/{self.output_file}', 'a') as results_file:
            results_file.write('insertion;insertion_first;'+str(ret['execution_time'])+'\n')    

        return ret

    def __operations_first(self):
        start = time.time()

        self.__create_indexes()
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')
        self.collection.insert_many_by_csv(self.source_folder, self.date_columns)
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }
        with open(f'{self.csv_destination}/{self.output_file}', 'a') as results_file:
            results_file.write('insertion;operations_first;'+str(ret['execution_time'])+'\n')    

        return ret

    def insert(self):
        if self.method == 'insertion_first':
            return self.__insert_first()
        elif self.method == 'operations_first':
            return self.__operations_first()
        else:
            raise BaseException('Method not implemented')
    
    def generate_domain_profile(self):        
        heterogeneous_domain = {}
        non_heterogeneous_domain = {}
        
        for operation_type in ['translation','merging','splitting']:
            fields = self.collection.collection.collection_versions.distinct('next_operation.field', {'next_operation.type':operation_type})

            for field in fields:
                values = self.collection.collection.collection_versions.distinct('next_operation.from', {'next_operation.type':operation_type, 'next_operation.field':field})
                values.extend(self.collection.collection.collection_versions.distinct('next_operation.to', {'next_operation.type':operation_type, 'next_operation.field':field}))

                heterogeneous_domain[field] = set(values)        

        mr = self.collection.collection.db.command('mapreduce', self.collectionname, map='function() { for (var key in this) { emit(key, null); } }', reduce='function(key, stuff) { return null; }', out=self.collectionname + '_keys')
        fields = self.collection.collection.db[self.collectionname + '_keys'].distinct('_id')
        
        for field in fields:
            if field.startswith('_'):
                continue
            values = self.collection.collection.collection.distinct(field)
            non_heterogeneous_domain[field] = set(values).difference(heterogeneous_domain.get(field, set()))

        self.collection.collection.db[self.collectionname + '_keys'].drop()

        field_types = {}

        for field in heterogeneous_domain:
            values = heterogeneous_domain[field]
            if all(isinstance(value, (int, float)) for value in values):
                field_types[field] = 'numeric'
            elif all(isinstance(value, str) for value in values):
                field_types[field] = 'text'
            elif all(isinstance(value, datetime) for value in values):
                field_types[field] = 'date'
            else:
                field_types[field] = 'mixed'

        for field in non_heterogeneous_domain:
            values = non_heterogeneous_domain[field]
            if all(isinstance(value, (int, float)) for value in values):
                field_types[field] = 'numeric'
            elif all(isinstance(value, str) for value in values):
                field_types[field] = 'text'
            elif all(isinstance(value, datetime) for value in values):
                field_types[field] = 'date'
            else:
                field_types[field] = 'mixed'

        return heterogeneous_domain, non_heterogeneous_domain, field_types    

    
    def generate_queries_list(self):
        queries_file = os.path.join(self.query_source, f'queries_{str(self.percent_of_heterogeneous_queries)}_{str(self.percent_of_insertions)}_{str(self.number_of_operations)}.txt')        

        if os.path.exists(queries_file):            
            return
                

        domain_dict_heterogeneous, domain_dict_nonheterogeneous, field_types = self.generate_domain_profile()

        updates = math.floor(self.number_of_operations*self.percent_of_insertions)
        reads = self.number_of_operations-updates

        heterogeneous_queries = math.floor(reads*self.percent_of_heterogeneous_queries)
        non_heterogeneous_queries = reads-heterogeneous_queries
        sequence = ([0]*heterogeneous_queries)
        sequence.extend([1]*non_heterogeneous_queries)

        heterogeneous_inserts = math.floor(updates*self.percent_of_heterogeneous_queries)
        non_heterogeneous_inserts = updates-heterogeneous_inserts
        sequence.extend([2]*heterogeneous_inserts)
        sequence.extend([3]*non_heterogeneous_inserts)
        
        
        random.shuffle(sequence)


        queries = []
        for s in sequence:            
            if s<=1:
                test_obj = {} ##Will be null, tests are only for insertions
                obj = {}
                for i in range(2):                    
                    if s==0 or i ==1:
                        ## Generate a non-heterogeneous query                    
                        field = random.choice(list(domain_dict_nonheterogeneous.keys()))
                        value = random.choice(list(domain_dict_nonheterogeneous[field]))
                    elif s==1:             
                        ## Generate a heterogeneous query                       
                        field = random.choice(list(domain_dict_heterogeneous.keys()))
                        value = random.choice(list(domain_dict_heterogeneous[field]))

                    if(field_types[field]=='date'):
                        value = value.isoformat()

                    if (field_types[field] == 'numeric' or field_types[field]=='date') and random.random() < 0.5: #50% of the time we will use a range query
                        r = random.random()                

                        if r < 0.5:
                            value = {'$gt':value}
                        else:
                            value = {'$lt':value}     

                    obj[field]  = value
            else:
                obj = {}
                test_obj = {}
                for field in domain_dict_heterogeneous.keys():
                    value = random.choice(list(domain_dict_heterogeneous[field]))
                    if(field_types[field]=='date'):
                        value = value.isoformat()
                    obj[field] = value
                for field in domain_dict_nonheterogeneous.keys():
                    value = random.choice(list(domain_dict_nonheterogeneous[field]))
                    if(field_types[field]=='date'):
                        value = value.isoformat()
                    obj[field] = value       

                heterogeneous_field = random.choice(list(domain_dict_heterogeneous.keys()))                
                test_obj[heterogeneous_field] = obj[heterogeneous_field]  ## Let's test in sequence if the queries envolving this heterogeneous fields were successfull

                non_heterogeneous_field = random.choice(list(domain_dict_nonheterogeneous.keys()))
                test_obj[non_heterogeneous_field] = obj[non_heterogeneous_field]  ## Let's test in sequence if the queries envolving this non-heterogeneous fields were successfull

            queries.append((str(s),obj,test_obj))        
                
        with open(queries_file, 'w') as file:
            file.write('type;query;test_query\n')
            for query in queries:
                file.write(f'{query[0]};{query[1]};{query[2]}\n')
    
    def generate_queries_list_fields(self):
        queries_file = os.path.join(self.query_source, f'queries_{str(self.percent_of_heterogeneous_queries)}_{str(self.percent_of_insertions)}_{str(self.number_of_operations)}.txt')        

        if os.path.exists(queries_file):            
            return
                

        domain_dict_heterogeneous, domain_dict_nonheterogeneous, field_types = self.generate_domain_profile()

        updates = math.floor(self.number_of_operations*self.percent_of_insertions)
        reads = self.number_of_operations-updates

        heterogeneous_queries = math.floor(reads*self.percent_of_heterogeneous_queries)
        non_heterogeneous_queries = reads-heterogeneous_queries
        sequence = ([0]*heterogeneous_queries)
        sequence.extend([1]*non_heterogeneous_queries)

        heterogeneous_inserts = math.floor(updates*self.percent_of_heterogeneous_queries)
        non_heterogeneous_inserts = updates-heterogeneous_inserts
        sequence.extend([2]*heterogeneous_inserts)
        sequence.extend([3]*non_heterogeneous_inserts)
        
        
        random.shuffle(sequence)


        queries = []
        for s in sequence:            
            if s<=1:
                test_obj = {} ##Will be null, tests are only for insertions
                obj = {}

                field1 = 'cid'
                field2 = 'municipio'
                                    
                if s==0:
                    ## Generate a non-heterogeneous query                    
                    
                    value1 = random.choice(list(domain_dict_nonheterogeneous[field1]))                    
                    value2 = random.choice(list(domain_dict_nonheterogeneous[field2]))
                elif s==1:             
                    ## Generate a heterogeneous query                                           
                    value1 = random.choice(list(domain_dict_heterogeneous[field1]))                    
                    value2 = random.choice(list(domain_dict_heterogeneous[field2]))

                obj[field1]  = value1
                obj[field2]  = value2
            else:
                obj = {}
                test_obj = {}
                for field in domain_dict_heterogeneous.keys():
                    value = random.choice(list(domain_dict_heterogeneous[field]))
                    if(field_types[field]=='date'):
                        value = value.isoformat()
                    obj[field] = value
                for field in domain_dict_nonheterogeneous.keys():
                    value = random.choice(list(domain_dict_nonheterogeneous[field]))
                    if(field_types[field]=='date'):
                        value = value.isoformat()
                    obj[field] = value       

                heterogeneous_field = random.choice(list(domain_dict_heterogeneous.keys()))                
                test_obj[heterogeneous_field] = obj[heterogeneous_field]  ## Let's test in sequence if the queries envolving this heterogeneous fields were successfull

                non_heterogeneous_field = random.choice(list(domain_dict_nonheterogeneous.keys()))
                test_obj[non_heterogeneous_field] = obj[non_heterogeneous_field]  ## Let's test in sequence if the queries envolving this non-heterogeneous fields were successfull

            queries.append((str(s),obj,test_obj))        
                
        with open(queries_file, 'w') as file:
            file.write('type;query;test_query\n')
            for query in queries:
                file.write(f'{query[0]};{query[1]};{query[2]}\n')
    

    def DecodeDateTime(self,empDict):
        for key, value in empDict.items():
            if isinstance(value, str) and re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '', value) == '':
                empDict[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return empDict

    def EncodeDateTime(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")

    def drop_database(self):
        client = MongoClient(self.host)
        client.drop_database(self.dbname)
        print('Database Dropped')

    def execute_queries(self):       
        queries_file = os.path.join(self.query_source, f'queries_{str(self.percent_of_heterogeneous_queries)}_{str(self.percent_of_insertions)}_{str(self.number_of_operations)}.txt')        

        queries = pd.read_csv(queries_file, sep=';')

        time_taken = 0.0

        with open(f'{self.csv_destination}/{self.output_file}', 'a') as results_file:
            results_file.write(f'Test Start;{self.operation_mode};{self.method};{str(self.number_of_operations)};{str(self.percent_of_insertions)};{str(self.percent_of_heterogeneous_queries)};{str(self.execution_try)}\n')
            results_file.write('operation_mode;type;query;hashed_result\n')            
            for idx,row in queries.iterrows():                
                query = row['query']
                operation_type = row['type']
                if operation_type <=1:                    

                    try:
                        query_str = query.replace('\'','\"')                
                        query = json.loads(query_str.strip(), object_hook=self.DecodeDateTime)
                        start = time.time()
                        query_result = self.collection.find_many(query)
                        end = time.time()                        
                    except BaseException as e:
                        results_file.write('Error;' + query_str.strip() + '\n')
                        continue
                    
                    time_taken += (end-start)
                    
                    results_file.write(f'{self.operation_mode};query;{query_str.strip()};;{str(end-start)}\n')                    

                    
                else:                                        

                    ##Insertion                    
                    try:
                        
                        query_str = query.replace('\'','\"')
                        query = json.loads(query_str.strip())
                        for key, value in query.items():
                            if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                                query[key] = datetime.fromisoformat(value)                                

                        start = time.time()                    
                        self.collection.insert_one(json.dumps(query, default=self.EncodeDateTime), query['RefDate'])                        
                        end = time.time()
                        time_taken += (end-start)
                        results_file.write(f'{self.operation_mode};insertion;{query_str.strip()};;{str(end-start)}\n')
                    except BaseException as e:
                        results_file.write('Error;' + query_str.strip() + '\n')
                        continue                    

            results_file.write('Time Taken;' + str(time_taken) + '\n')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run database operations experiments.")
    parser.add_argument("--host", type=str, default="localhost", help="MongoDB host")
    parser.add_argument("--dbname", type=str, required=True, help="Database name")
    parser.add_argument("--collectionname", type=str, required=True, help="Collection name")
    parser.add_argument("--sourcefolder", type=str, required=True, help="Source folder for CSV files")
    parser.add_argument("--csvdestination", type=str, required=True, help="Destination folder for results")
    parser.add_argument("--datecolumn", type=str, required=True, help="Date column in the source CSV")
    parser.add_argument("--approach", type=str, required=True, help="Approach to use. Options: 'preprocess','rewrite'",choices=["preprocess", "rewrite"])
    parser.add_argument("--method", type=str, default="insertion_first", 
                        choices=["insertion_first", "operations_first"], 
                        help="Method to execute. Options: 'insertion_first', 'operations_first'")
    parser.add_argument("--operationsfile", type=str, required=True, help="CSV file with operations")
    parser.add_argument("--trials", type=int, nargs=2, required=True, help="Range of trials to execute (start and end)")
    parser.add_argument("--numberofoperations", type=int, nargs=3, metavar=('start', 'end', 'pace'), 
                        help="Range for number of operations: start, end, and pace")    
    parser.add_argument("--indexes", type=str, nargs="*", default=None, help="List of indexes to create")    
    parser.add_argument("--querysource", type=str, required=True, help="Path to read and write the queries from")
    
    
    args = parser.parse_args()

    host = args.host
    dbname = args.dbname
    collectionname = args.collectionname
    source_folder = args.sourcefolder
    date_columns = args.datecolumn
    csv_destination = args.csvdestination
    operations_file = args.operationsfile
    trials = args.trials
    indexes = args.indexes
    method = args.method
    number_of_operations_a = args.numberofoperations
    operation_mode = args.approach
    trials_start, trials_end = args.trials
    query_source = args.querysource
    

    rebuild = True

    with open(f"{csv_destination}/experiment_log.txt", "w") as log_file:
        for execution_try in range(trials_start, trials_end + 1):
            for number_of_operations in range(number_of_operations_a[0], number_of_operations_a[1] + 1, number_of_operations_a[2]):
                for percent_of_heterogeneous_queries in [0.15,0.3]:
                    for percent_of_insertions in [0,0.05,0.5,0.95,1]:

                        output_file = f'results_{str(percent_of_heterogeneous_queries)}_{str(percent_of_insertions)}_{str(number_of_operations)}_{str(operation_mode)}_{str(execution_try)}.txt'                            
                        
                        if os.path.exists(f"{csv_destination}/{output_file}"):
                            print(f"File {output_file} already exists. Skipping...")
                            log_file.write(f"File {output_file} already exists. Skipping...\n")
                            continue

                        try:
                            print(f"Starting execution for {output_file}...")
                            log_file.write('Executing ' + output_file)
                            c = Comparator(
                                host, 
                                operation_mode, 
                                method, 
                                dbname, 
                                collectionname, 
                                source_folder, 
                                date_columns, 
                                csv_destination,operations_file,
                                number_of_operations, 
                                percent_of_heterogeneous_queries, 
                                percent_of_insertions, 
                                execution_try,                                      
                                output_file,
                                indexes,
                                query_source
                            )

                            if rebuild:                                
                                log_file.write('Inserting Data\n') 
                                log_file.flush()
                                c.insert()
                                rebuild = False                                                                                                   
                        
                            log_file.write('Generating Queries\n')
                            log_file.flush()
                            c.generate_queries_list()
                            c.generate_queries_list_fields()

                            log_file.write('Executing Queries\n')
                            log_file.flush()
                            c.execute_queries()                            
                            log_file.write(f'Finished {output_file}\n')
                            log_file.flush()
                            time.sleep(10)
                        except BaseException:
                            log_file.write('Error executing')
                            log_file.flush()                     

        rebuild = True
        c.drop_database()