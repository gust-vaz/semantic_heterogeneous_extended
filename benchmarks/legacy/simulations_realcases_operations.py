import argparse
import os
import time
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from semantic_heterogeneous_database import BasicCollection
import re
pd.options.mode.chained_assignment = None 

class Comparator:
    def __init__(self, host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, output_file, indexes=None):
        self.operation_mode = operation_mode
        self.method = method
        self.dbname = dbname
        self.collectionname = collectionname
        self.source_folder = source_folder
        self.date_columns = date_columns
        self.csv_destination = csv_destination
        self.operations_file = operations_file
        self.host = host
        self.collection = BasicCollection(self.dbname, self.collectionname, self.host, self.operation_mode)
        self.output_file = output_file
        self.indexes = indexes or []  # Default to an empty list if None

        os.makedirs(csv_destination, exist_ok=True)

    def __create_indexes(self):
        if self.indexes:       
            print(f'Creating indexes: {self.indexes}')     
            self.collection.create_index(self.indexes)

    def __insert_first(self):            
        start = time.time()

        self.__create_indexes()
        self.collection.insert_many_by_csv(self.source_folder, self.date_columns)        
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')        
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }

        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
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
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run database operations experiments.")
    parser.add_argument("--host", type=str, default="localhost", help="MongoDB host")
    parser.add_argument("--dbname", type=str, required=True, help="Database name")
    parser.add_argument("--collectionname", type=str, required=True, help="Collection name")
    parser.add_argument("--sourcefolder", type=str, required=True, help="Source folder for CSV files")
    parser.add_argument("--datecolumn", type=str, required=True, help="Date column in the source CSV")
    parser.add_argument("--csvdestination", type=str, required=True, help="Destination folder for results")
    parser.add_argument("--operationsfile", type=str, required=True, help="CSV file with operations")
    parser.add_argument("--trials", type=int, nargs=2, required=True, help="Range of trials to execute (start and end)")
    parser.add_argument("--indexes", type=str, nargs="*", default=None, help="List of indexes to create")
    parser.add_argument("--methods", type=str, nargs="*", default=["insertion_first", "operations_first"], 
                        choices=["insertion_first", "operations_first"], 
                        help="List of methods to execute. Options: 'insertion_first', 'operations_first'")
    
    args = parser.parse_args()

    host = args.host
    dbname = args.dbname
    collectionname = args.collectionname
    source_folder = args.sourcefolder
    date_columns = args.datecolumn
    csv_destination = args.csvdestination
    operations_file = args.operationsfile
    trials_start, trials_end = args.trials
    indexes = args.indexes
    methods = args.methods

    os.makedirs(csv_destination, exist_ok=True)

    with open(f"{csv_destination}/experiment_log.txt", "w") as log_file:
        for method in methods:
            for execution_try in range(trials_start, trials_end + 1):
                output_file = f"results_operations_{method}_{str(execution_try)}.txt"

                try:
                    print(f"Starting execution for {output_file}...")
                    log_file.write(f"Executing {output_file}\n")
                    log_file.flush()
                    c = Comparator(
                        host,
                        "preprocess",
                        method,
                        dbname,
                        collectionname,
                        source_folder,
                        date_columns,
                        csv_destination,
                        operations_file,
                        output_file,
                        indexes=indexes
                    )
                    print("Inserting data...")
                    log_file.write("Inserting Data\n")
                    log_file.flush()
                    c.insert()
                    print(f"Finished execution for {output_file}.")
                    log_file.write(f"Finished {output_file}\n")
                    log_file.flush()
                    time.sleep(10)
                except BaseException as e:
                    print(f"Error during execution for {output_file}: {str(e)}")
                    log_file.write(f"Error executing {output_file}: {str(e)}\n")
                    log_file.flush()
                finally:
                    print("Dropping database...")
                    c.drop_database()
                    print("Database dropped.")
