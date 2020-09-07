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
    if operator == ">=":
        return value1 >= value2


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
        # self.index[key_field_name] = {}

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
            self.index[i][values[i]] = filename, self.counterRecords % 1500

    def delete_record(self, key: Any) -> None:
        file, pos = self.search(key)
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
                type_ = self.fields[indexOfField].type
                prev = self.index[i].pop(type_(csv_reader[pos][indexOfField]))
                self.index[i][type_(lastRecord[0][indexOfField])] = prev

            csv_reader = readFromFile(file)
            csv_reader[pos] = lastRecord[0]

            writeToFile(self.files[-1], 'w', csv_reader)
        else:
            for i in self.index:
                indexOfField = [j for j, f in enumerate(self.fields) if f.name == i][0]
                type_ = self.fields[indexOfField].type
                self.index[i].pop(type_(csv_reader[pos][indexOfField]))

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
                    if not calculateOp(
                            type(c.value)(row[self.fields.index(db_api.DBField(c.field_name, type(c.value)))]), c.value,
                            c.operator):
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
            # rows = readFromFile(file)[1:]
            # for i, row in enumerate(rows):
            #   self.tables[tableName].index[self.tables[tableName].key_field_name][row[0]] = (file, i + 1)

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

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[db_api.SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        final_res = []
        match_records = []
        for index_table in range(len(tables)):
            match_records.append(self.get_table(tables[index_table]).query_table(fields_and_values_list[index_table]))
        if len(match_records) < 2:
            return match_records[0]
        for i in range(1, len(match_records)):
            final_res = []
            for record1 in match_records[i - 1]:
                for record2 in match_records[i]:
                    for field in fields_to_join_by:
                        if record1.get(field) != record2.get(field):
                            break
                    else:
                        record1.update(record2)
                        final_res.append(record1)
            match_records[i] = final_res
        return final_res

        '''


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
print(table.query_table([db_api.SelectionCriteria("age", "=", 5)]))
print(table.search(315437145))
print(table.get_record(315437145))
print(table.get_record(211426754))
# table.delete_record(315437145)
table.update_record(211426754, {'age': 21})
table.update_record(315437145, {"name": "Malka Pnina"})
'''


'''

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
    print([(j, table.index[i][j]) for j in table.index[i]])
table.delete_record(333094951)
for i in table.index:
    print(i)
    print([(j, table.index[i][j]) for j in table.index[i]])
    '''
'''
my_db = DataBase()
fields_animals = [db_api.DBField("aid", str), db_api.DBField("aname", str), db_api.DBField("age", int),
                  db_api.DBField("species", str)]
fields_employees = [db_api.DBField("pid", str), db_api.DBField("pname", str), db_api.DBField("contact", int),
                    db_api.DBField("species", str), db_api.DBField("age", int)]
animals = my_db.create_table("animals", fields_animals, "aid")
employees = my_db.create_table("employees", fields_employees, "pid")
a1_dict = {"aid": "a1",
           "aname": "aaa",
           "age": 5,
           "species": "lion"}
a2_dict = {"aid": "a2",
           "aname": "bbb",
           "age": 7,
           "species": "tiger"}
a3_dict = {"aid": "a3",
           "aname": "ddd",
           "age": 7,
           "species": "lion"}
p1_dict = {"pid": "p1",
           "pname": "Bob",
           "contact": 123456,
           "species": "lion",
           "age": 7}
p2_dict = {"pid": "p2",
           "pname": "Dan",
           "contact": 55555,
           "species": "lion",
           "age": 5}
p3_dict = {"pid": "p3",
           "pname": "Danniel",
           "contact": 987654,
           "species": "tiger",
           "age": 7}
animals.insert_record(a1_dict)
# print(animals.count())
animals.insert_record(a2_dict)
# print(animals.count())
animals.create_index("age")
# animals.create_index("species")
animals.insert_record(a3_dict)
# print(animals.count())
# animals.delete_record("a1")
# print(animals.get_record("a2"))
# a2_dict = {"aid": "a2",
# "aname": "bbb",
# "age": 7,
# "species": "dog"}
# animals.update_record("a2", a2_dict)
employees.insert_record(p1_dict)
employees.insert_record(p2_dict)
employees.insert_record(p3_dict)
# print(my_db.num_tables())
# print("table names:")
# print(my_db.get_tables_names())
# my_db.delete_table("employees")
# print("after delete:")
# print(my_db.num_tables())
c1 = db_api.SelectionCriteria("age", '>=', 5)
c2 = db_api.SelectionCriteria("species", '=', "lion")
# print(animals.query_table([c1]))
# animals.delete_records([c1])
# print(animals.count())
c3 = db_api.SelectionCriteria("contact", '>=', 1)
c4 = db_api.SelectionCriteria("age", '=', 5)
print(my_db.query_multiple_tables(["employees", "animals"], [[c3], [c4]], ["species"]))
# print(my_db.query_multiple_tables(["employees", "animals"],[c3, c4] ,["species", "age"]))
'''