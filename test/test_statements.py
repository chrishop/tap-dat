import unittest
from tap_dat.statements import Statements

class TestStatements(unittest.TestCase):
    
    def test_create(self):
        table_column_name = "test_table"
        columns = [{ 'column_name': 'column1', 'datatype': 'BIGINT'},
                   { 'column_name': 'column2', 'datatype': 'VARCHAR(255)'}]
        
        expected = "CREATE TABLE test_table (column1 BIGINT,column2 VARCHAR(255))"
        
        self.assertEqual(Statements.create(table_column_name, columns), expected)
        
    def test_insert(self):
        table_name = "test_table"
        column_names = ['id', 'column_name', 'dob']
        
        expected = "INSERT INTO test_table (id,column_name,dob) VALUES (%(id)s,%(column_name)s,%(dob)s)"
                    
        self.assertEqual(Statements.insert(table_name, column_names), expected)