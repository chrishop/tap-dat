import unittest
from tap_dat.local_database.statements import Statements

class TestStatementGenerator(unittest.TestCase):
    
    def test_create(self):
        table_name = "test_table"
        columns = [{ 'name': 'column1', 'type': 'BIGINTEGER'},
                   { 'name': 'column2', 'type': 'VARCHAR(255)'}]
        
        expected = "CREATE TABLE test_table (column1 BIGINTEGER,column2 VARCHAR(255))"
        
        self.assertEqual(Statements.create(table_name, columns), expected)
        
    def test_insert(self):
        table_name = "test_table"
        column_names = ['id', 'name', 'dob']
        
        expected = "INSERT INTO test_table (id,name,dob) VALUES (%(id)s,%(name)s,%(dob)s)"
                    
        self.assertEqual(Statements.insert(table_name,column_names), expected)