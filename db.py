import os
from abc import ABC

import db_api
import csv
from typing import Any, Dict, List, Type


def readFromFile(file_name: str) -> list:
    with open(file_name, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        return list(csv_reader)


def writeToFile(file_name: str, mode: str, rows: List[list]) -> None:
    with open(file_name, mode, newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        for row in rows:
            csv_writer.writerow(row)


class DBTable(db_api.DBTable):
    """ctor"""
    '''
    def __init__(self, file):
        self.name = file[-5]
        self.fields = readFromFile()'''
    def __init__(self):
        pass

    def createNew(self, name: str, field: List[db_api.DBField], key_field_name: str):
        self.name = name
        self.fields = field[:]
        self.key_field_name = key_field_name
        self.counterRecords = 0
        self.files = []
        self.files.append(os.path.abspath(os.path.join("db_files", f'{name}1.csv')))
        writeToFile(self.files[0], 'w', [self.fields])

    """tmp"""
    def search(self, key: Any) -> (str, int):
        for file in self.files:
            csv_reader = readFromFile(file)
            for i, row in enumerate(csv_reader):
                if row[0] == str(key):
                    return file, i
        return None, 0

    def checkTheKey(self, key: Any) -> bool:
        return not self.search(key)[0] is None

    '''members'''

    def count(self) -> int:
        return self.counterRecords
        # raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.checkTheKey(values[self.key_field_name]):
            print("The key is already exist")
            raise ValueError
        filename = self.files[-1]
        self.counterRecords += 1

        if self.counterRecords % 1500 == 0:
            filename = f'db_files/{self.name}{len(self.files) + 1}.csv'
            self.files.append(filename)
            writeToFile(filename, 'w', [[i.name for i in self.fields]])

        writeToFile(filename, 'a', [[values.get(field.name) for field in self.fields]])
        # raise NotImplementedError

    def delete_record(self, key: Any) -> None:

        file, index = self.search(key)
        if file is None:
            raise ValueError
        self.counterRecords -= 1
        lastRecord = []
        if file is None:
            print("The record isn't exist")
            return
        csv_reader = readFromFile(self.files[-1])
        lastRecord.append(csv_reader[-1])
        writeToFile(self.files[-1], 'w', csv_reader[:-1])
        if lastRecord[0][0] != str(key):
            csv_reader = readFromFile(file)
            csv_reader[index] = lastRecord[0]
            writeToFile(self.files[-1], 'w', csv_reader)
        # raise NotImplementedError

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        file, line_number = self.search(key)
        if file is None:
            return {}
        rows = readFromFile(file)
        my_row = rows[line_number]
        toReturn = {}
        for i, field in enumerate(self.fields):
            toReturn[field.name] = my_row[i]
        return toReturn

        # raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        file, index = self.search(key)
        if file is None:
            return
        rows = readFromFile(file)
        row = rows[index]
        for i, field in enumerate(self.fields):
            if values.get(field.name):
                row[i] = values[field.name]
        writeToFile(file, 'w', rows)
        # raise NotImplementedError

    def query_table(self, criteria: List[db_api.SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError



class DataBase(db_api.DataBase):
    def __init__(self):
        self.tables = {}
        self.numOfTables = 0
        for file in os.listdir("db_files"):
            tableName = file[:-5]
            file = os.path.abspath(os.path.join("db_files", file))
            if tableName in self.tables.keys():
                self.tables[tableName].files.append(file)
                self.tables[tableName].counterRecords += len(readFromFile(file)) - 1
            else:
                self.tables[tableName] = DBTable()
                self.tables[tableName].name = tableName
                read = readFromFile(file)
                self.tables[tableName].fields = read[0]
                self.tables[tableName].key_field_name = read[0][0]
                self.tables[tableName].counterRecords = len(read) - 1
                self.tables[tableName].files = [file]
                self.numOfTables += 1

    # Put here any instance information needed to support the API
    def create_table(self,
                     table_name: str,
                     fields: List[db_api.DBField],
                     key_field_name: str) -> DBTable:

        self.tables[table_name] = DBTable()
        self.tables[table_name].createNew(table_name, fields, key_field_name)

        self.numOfTables += 1
        return self.tables[table_name]

    # raise NotImplementedError

    def num_tables(self) -> int:
        return self.numOfTables
        #raise NotImplementedError

    def get_table(self, table_name: str) -> DBTable:
        return self.tables[table_name]
        #raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
        for file in self.tables[table_name].files:
            os.remove(file)
        del self.tables[table_name]
        self.numOfTables -= 1
        #raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        return list(self.tables.keys())
        #raise NotImplementedError

    '''def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError'''

'''
field1 = db_api.DBField("name", str)
field2 = db_api.DBField("age", int)
field3 = db_api.DBField("id", int)

table = DBTable()
table.createNew("Person", [field3, field1, field2], "id")
table.insert_record({"name": "Pnini", "age": 20, "id": 315437145})
table.insert_record({"name": "Shira", "age": 20, "id": 211426754})

print(table.search(315437145))
print(table.get_record(315437145))
print(table.get_record(211426754))
# table.delete_record(315437145)
table.update_record(211426754, {'age': 21})
table.update_record(315437145, {"name": "Malka Pnina"})'''
