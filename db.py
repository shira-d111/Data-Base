import os
from BTrees.OOBTree import OOBTree


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


def calculateOp(value1: Any, value2: Any, operator: str) -> bool:
    if operator == "=":
        return value1 == value2
    if operator == ">":
        return value1 > value2
    if operator == "<":
        return value1 < value2


def orderTheKey(field: List[db_api.DBField], key_field_name: str) -> None:
    key_index = [i for i, f in enumerate(field) if f.name == key_field_name]
    if len(key_index) == 0:
        raise ValueError
    field[0], field[key_index[0]] = field[key_index[0]], field[0]


class DBTable(db_api.DBTable):
    """ctor"""

    def __init__(self):
        self.index = {}
        self.files = []
        self.counterRecords = 0

    def createNew(self, name: str, field: List[db_api.DBField], key_field_name: str):
        """make index to key"""

        orderTheKey(field, key_field_name)
        self.name = name
        self.fields = field[:]
        self.key_field_name = key_field_name
        self.files.append(os.path.abspath(os.path.join("db_files", f'{name}1.csv')))
        writeToFile(self.files[0], 'w', [self.fields])
        #self.index[key_field_name] = {}

    def search(self, key: Any) -> (str, int):
        if self.key_field_name in self.index:
            file = self.index[self.key_field_name].get(key)
            if type(file) == tuple:
                return file
            return None, 0
        for file in self.files:
            csv_reader = readFromFile(file)
            for i, row in enumerate(csv_reader):
                if row[0] == str(key):
                    return file, i
        return None, 0

    def checkTheKey(self, key: Any) -> bool:
        return not self.search(key)[0] is None

    def count(self) -> int:
        return self.counterRecords

    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.checkTheKey(values[self.key_field_name]):
            raise ValueError
        filename = self.files[-1]
        self.counterRecords += 1
        if self.counterRecords % 1500 == 0:
            filename = f'db_files/{self.name}{len(self.files) + 1}.csv'
            self.files.append(filename)
            writeToFile(filename, 'w', [[i.name for i in self.fields]])

        writeToFile(filename, 'a', [[values.get(field.name) for field in self.fields]])
        for i in self.index:
            self.index[i][values[i]] = filename,  self.counterRecords % 1500

    def delete_record(self, key: Any) -> None:
        file, index = self.search(key)
        if file is None:
            raise ValueError
        self.counterRecords -= 1
        lastRecord = []
        csv_reader = readFromFile(self.files[-1])
        lastRecord.append(csv_reader[-1])
        writeToFile(self.files[-1], 'w', csv_reader[:-1])
        if lastRecord[0][0] != str(key):
            for i in self.index:
                indexOfField = [j for j, f in enumerate(self.fields) if f.name == i][0]
                self.index[i].pop(int(csv_reader[index][indexOfField]))
            csv_reader = readFromFile(file)
            csv_reader[index] = lastRecord[0]
            writeToFile(self.files[-1], 'w', csv_reader)

    def delete_records(self, criteria: List[db_api.SelectionCriteria]) -> None:
        for file in self.files:
            rows = readFromFile(file)
            rows = rows[1:]
            for row in rows:
                flag = True
                for c in criteria:
                    if not calculateOp(
                            type(c.value)(row[self.fields.index(db_api.DBField(c.field_name, type(c.value)))]), c.value,
                            c.operator):
                        flag = False
                if flag:
                    self.delete_record(row[0])

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

    def query_table(self, criteria: List[db_api.SelectionCriteria]) -> List[Dict[str, Any]]:
        listToReturn = []
        for file in self.files:
            rows = readFromFile(file)
            rows = rows[1:]
            for row in rows:
                flag = True
                for c in criteria:
                    if not calculateOp(type(c.value)(row[self.fields.index(db_api.DBField(c.field_name, type(c.value)))]), c.value, c.operator):
                        flag = False

                if flag:
                    my_dic = {}
                    for i, field in enumerate(self.fields):
                        my_dic[field.name] = row[i]
                    listToReturn.append(my_dic)
        return listToReturn

    def create_index(self, field_to_index: str) -> None:
        if field_to_index in self.index:
            print("There is already exist")
            return
        index = [i for i, f in enumerate(self.fields) if f.name == field_to_index][0]
        my_type = [f.type for f in self.fields if f.name == field_to_index][0]
        self.index[field_to_index] = OOBTree()
        for file in self.files:
            num = 1
            rows = readFromFile(file)[1:]
            for row in rows:
                self.index[field_to_index][my_type(row[index])] = (file, num)
                num += 1


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
    def create_table(self, table_name: str, fields: List[db_api.DBField], key_field_name: str) -> DBTable:
        self.tables[table_name] = DBTable()
        self.tables[table_name].createNew(table_name, fields, key_field_name)

        self.numOfTables += 1
        return self.tables[table_name]

    def num_tables(self) -> int:
        return self.numOfTables

    def get_table(self, table_name: str) -> DBTable:
        return self.tables[table_name]

    def delete_table(self, table_name: str) -> None:
        for file in self.tables[table_name].files:
            os.remove(file)
        del self.tables[table_name]
        self.numOfTables -= 1

    def get_tables_names(self) -> List[Any]:
        return list(self.tables.keys())

'''
    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError'''


field1 = db_api.DBField("name", str)
field2 = db_api.DBField("age", int)
field3 = db_api.DBField("id", int)
table = DBTable()
table.createNew("Person", [field3, field1, field2], "id")
table.insert_record({"name": "Pnini", "age": 5, "id": 315437145})
table.insert_record({"name": "Shira", "age": 4, "id": 211426754})
table.create_index("age")
table.insert_record({"name": "Elisheva", "age": 11, "id": 333094951})
for i in table.index:
    print(i)
    print(list(table.index[i].keys()))
    print(list(table.index[i].values()))
table.delete_record(315437145)
for i in table.index:
    print(i)
    print(list(table.index[i].keys()))
    print(list(table.index[i].values()))

print("***************")
print(table.query_table([db_api.SelectionCriteria("age", "=", 4)]))
print(table.search(315437145))
print(table.get_record(315437145))
print(table.get_record(211426754))
table.update_record(211426754, {'age': 21})
print(table.get_record(211426754))

