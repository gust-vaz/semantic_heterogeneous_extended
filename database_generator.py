from semantic_heterogeneous_database import BasicCollection
from datetime import datetime
import random
import string


### This class first randomically generates the documents to be inserted, and the semantic operations for the database
### Insertion is only executed later. This way, performance tests can be executed without any delay caused by the generation

class DatabaseGenerator:
    FIELD_TYPES = ['int', 'float', 'datetime','string']
    OPERATION_TYPE = ['merging', 'translation','splitting']

    def __init__(self, host='localhost'):
        self.host = host     
        self.operations = list() ## List to store randomly generated operations
        self.records = list()
        self.versions_dates = list()
        self.evoluted_values = dict()

    def __generate_field_domain(self, field_type, number_of_values_in_domain):        
        return_list = list()

        for i in range(number_of_values_in_domain):
            if field_type == 'datetime':
                value = datetime.fromordinal(random.randint(365*2000, 365*2100)).strftime('%Y-%m-%d')
            elif field_type == 'int':
                value = random.randint(1,9999999)
            elif field_type == 'float':
                value = random.randint(1,9999999)/random.randint(1,9999999)
            elif field_type == 'string':
                value = ''.join(random.choice(self.letters) for i in range(1,35))
            return_list.append(value)

        return return_list

    ## Pensar depois em uma distribuição para o número de campos ao inves de ser fixo
    #  alem de um numero delimitado de valores possiveis para os campos
    def generate_record(self):
        new_record = {}                

        for field in self.fields:
            new_record[field[0]] = random.choice(self.field_domain[field[0]])            
        
        new_record['valid_from_date']=datetime.fromordinal(random.randint(365*2000, 365*2100))

        self.records.append(new_record)  
        return new_record      

    def __check_evolution(self, fieldName, value):
        if fieldName in self.evoluted_values:
            if value in self.evoluted_values[fieldName]:
                return False
        
        self.evoluted_values.setdefault(fieldName, set())
        self.evoluted_values[fieldName].add(value)
        return True


    def generate_version(self):
        version_date = datetime.fromordinal(random.randint(365*2000, 365*2100))
        operation_type = random.choice(DatabaseGenerator.OPERATION_TYPE)
        arguments = None

         #float fields are not suitable for goruping nor translation
        if operation_type == 'translation':
            fieldName = random.choice(self.evolution_fields)[0]
            oldValue = random.choice(self.field_domain[fieldName])                        
            
            t = 0
            while not self.__check_evolution(fieldName, oldValue):                
                t=t+1
                oldValue = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()##Ran into an infinite loop here. Just try to generate another combination
                    return

            t = 0
            newValue = oldValue                    
            while newValue == oldValue:
                t=t+1
                newValue = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()##Ran into an infinite loop here. Just try to generate another combination
                    return
                
                t2 = 0
                while not self.__check_evolution(fieldName, newValue):
                    t2 = t2+1                    
                    newValue = random.choice(self.field_domain[fieldName])            
                    if t2>30:
                        self.generate_version()
                        return
            
            arguments = {
                'fieldName' : fieldName,
                'oldValue' : oldValue,
                'newValue' : newValue
            }
            
        elif operation_type == 'merging':            
            field = random.choice(self.evolution_fields) 
            fieldName = field[0]
            oldValues = [random.choice(self.field_domain[fieldName]), random.choice(self.field_domain[fieldName])]

            t=0
            while not self.__check_evolution(fieldName, oldValues[0]):
                t=t+1
                oldValues[0] = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()
                    return
                
            t=0
            while not self.__check_evolution(fieldName, oldValues[1]):
                t=t+1
                oldValues[1] = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()
                    return

            newValue = random.choice(self.field_domain[fieldName])         

            t=0
            while newValue in oldValues:
                t=t+1
                newValue = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()
                    return

                t2=0
                while not self.__check_evolution(fieldName, newValue):
                    t2=t2+1
                    newValue = random.choice(self.field_domain[fieldName])       
                    if t2>30:
                        self.generate_version()
                        return

            arguments = {
                'fieldName' : fieldName,
                'oldValues' : oldValues,
                'newValue' : newValue
            }
        
        elif operation_type == 'splitting':
            field = random.choice(self.evolution_fields) 
            fieldName = field[0]            
            oldValue = random.choice(self.field_domain[fieldName])      

            t=0
            while not self.__check_evolution(fieldName, oldValue):
                t=t+1
                oldValue = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()
                    return

            newValues = [random.choice(self.field_domain[fieldName]), random.choice(self.field_domain[fieldName])]

            t=0
            while not self.__check_evolution(fieldName, newValues[0]):
                t=t+1
                newValues[0] = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()
                    return
            
            t=0
            while not self.__check_evolution(fieldName, newValues[1]):
                t=t+1
                newValues[1] = random.choice(self.field_domain[fieldName])
                if t>30:
                    self.generate_version()
                    return

            arguments = {
                'fieldName' : fieldName,
                'oldValue' : oldValue,
                'newValues' : newValues
            }

        self.versions_dates.append(version_date)
        self.operations.append((operation_type, version_date, arguments))       


    def generate(self, number_of_records, number_of_versions, number_of_fields, number_of_values_in_domain, number_of_evolution_fields, operation_mode):
        ## Starting random database
        self.letters = string.ascii_lowercase
        self.database_name = ''.join(random.choice(self.letters) for i in range(5))
        self.collection_name = ''.join(random.choice(self.letters) for i in range(10))

        ##Generating fields present in the documents        
        self.fields = list()
        self.field_domain = dict()
        for i in range(number_of_fields):
            field_name = ''.join(random.choice(self.letters) for a in range(5))
            field_type = random.choice(DatabaseGenerator.FIELD_TYPES)
            self.fields.append((field_name, field_type))
            ##Generating fields domain of available values for each field. 
            self.field_domain[field_name] = self.__generate_field_domain(field_type, number_of_values_in_domain)

        fieldsList = list(filter(lambda f: f[1] != 'float', self.fields)) # float fields are not suitable for merging and splitting
        self.evolution_fields = [random.choice(fieldsList) for i in range(number_of_evolution_fields)]
        
        self.collection = BasicCollection(self.database_name, self.collection_name, self.host, operation_mode)

        self.versions_dates.append(datetime(1700,1,1))
             
        for i in range(number_of_versions-1):
            self.generate_version()  

        for i in range(number_of_records):
            self.generate_record()        

    def destroy(self):
        self.collection.collection.client.drop_database(self.collection.collection.database_name)
    


# import time
# #from database_generator import DatabaseGenerator

# d = DatabaseGenerator()
# d.generate(200, 5, 11,20)            