import multiprocessing
import shutil
from .Collection import Collection
from .GroupingOperation import GroupingOperation
from .TranslationOperation import TranslationOperation
from datetime import datetime

from .UngroupingOperation import UngroupingOperation
import os

class BasicCollection:
    def __init__(self, DatabaseName, CollectionName,
                 mongo_uri='mongodb://localhost:27017',
                 operation_mode='preprocess',
                 write_concern='majority'):
        if not isinstance(operation_mode, str) or operation_mode not in ['preprocess','rewrite']:
            raise BaseException('Operation Mode not recognized')

        self.operation_mode = operation_mode
        self.database_name = DatabaseName
        self.collection_name = CollectionName
        self.mongo_uri = mongo_uri
        self.write_concern = write_concern

        self.initialize_collection()


    def initialize_collection(self):
        self.collection = Collection(self.database_name, self.collection_name,
                                     self.mongo_uri, self.operation_mode,
                                     self.write_concern)

        # Register operations with new names
        self.collection.register_operation('translation', TranslationOperation(self))
        self.collection.register_operation('merging', GroupingOperation(self))
        self.collection.register_operation('splitting', UngroupingOperation(self))
        
        # Register operations with old names for backward compatibility
        self.collection.register_operation('grouping', GroupingOperation(self))
        self.collection.register_operation('ungrouping', UngroupingOperation(self))

    def insert_one(self, JsonString, ValidFromDate:datetime):
        self.collection.insert_one(JsonString, ValidFromDate)

    def insert_many_by_dataframe(self, dataframe, ValidFromField):
        self.collection.insert_many_by_dataframe(dataframe, ValidFromField)
    
    def insert_many_by_csv(self, FilePath, ValidFromField, ValidFromDateFormat='%Y-%m-%d', Delimiter=','):
        source_folder = os.path.dirname(FilePath)

        temp_destination = source_folder + '/temp/'
        shutil.rmtree(temp_destination, ignore_errors=True)
        os.makedirs(temp_destination, exist_ok=True)
        
        for file in os.listdir(source_folder):
            # Check if the file is a CSV file
            if file.endswith('.csv'):
                # Print the full file path
                file_path = os.path.join(source_folder, file)
                file_size = os.path.getsize(file_path)
                max_file_size = 5 * 1024 * 1024  # 15Mb in bytes

                if file_size > max_file_size:
                    # Divide the file into smaller files
                    chunk_size = max_file_size
                    with open(file_path, 'rb') as f:
                        header = f.readline()  # Read the first row of the original file
                        chunk = f.read(chunk_size)  # Read the rest of the chunk
                        chunk_number = 1
                        while chunk:
                            # Check if the chunk ends in the middle of a row
                            if chunk[-1] != b'\n':
                                # Find the last complete row in the chunk
                                last_row_index = chunk.rfind(b'\n')
                                # Trim the chunk to the last complete row
                                resto = chunk[last_row_index+1:]
                                chunk = chunk[:last_row_index+1]
                            # Write the chunk to a new file
                            new_file_path = os.path.join(temp_destination, f"{file}_{chunk_number}")
                            with open(new_file_path, 'wb') as new_file:
                                new_file.write(header)
                                new_file.write(chunk)
                            chunk_number += 1
                            # Read the next chunk
                            chunk = resto + f.read(chunk_size)
                else:
                    shutil.copy2(file_path, temp_destination)

            
        
        # Insert the entire file
        def insert_file(file_path, ValidFromField, ValidFromDateFormat, Delimiter):            
            self.collection.insert_many_by_csv(file_path, ValidFromField, ValidFromDateFormat, Delimiter)

        for file in sorted(os.listdir(temp_destination)):
            # Insert the entire file
            file_path = os.path.join(temp_destination, file)
            print('Inserting file:', file)
            insert_file(file_path, ValidFromField, ValidFromDateFormat, Delimiter)
            
            
            # p = multiprocessing.Process(target=insert_file, args=)
            # p.start()
            # p.join()  # Wait for the process to complete
            # p.terminate()  # Ensure the process is terminated

        shutil.rmtree(temp_destination)
        
    
    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def find_many(self, QueryString):
        return self.collection.find_many(QueryString)   

    def count_documents(self, QueryString):
        return self.collection.count_documents(QueryString)

    def pretty_print(self, recordsCursor):
        return self.collection.pretty_print(recordsCursor)

    def execute_operation(self, operationType, validFrom, args):
        self.collection.execute_operation(operationType, validFrom, args)

    def execute_many_operations_by_csv(self, filePath, operationTypeColumn, validFromField):
        self.collection.execute_many_operations_by_csv(filePath, operationTypeColumn, validFromField)    

    def create_index(self, fields):
        self.collection.create_index(fields)