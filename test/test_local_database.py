
import unittest
from tap_dat.local_database import LocalDatabase

class TestLocalDatabase(unittest.TestCase):
    
    table_spec = [  { 'column_name': 'string_column', 'datatype': 'CHAR(255)' },
                    { 'column_name': 'variable_string_column', 'datatype': 'VARCHAR(255)'},
                    { 'column_name': 'integer_column', 'datatype': 'INTEGER'},
                    { 'column_name': 'smallint_column', 'datatype': 'SMALLINT'},
                    { 'column_name': 'bigint_column', 'datatype': 'BIGINT'},
                    { 'column_name': 'double_column', 'datatype': 'DOUBLE'},
                    { 'column_name': 'real_column', 'datatype': 'REAL'}]
    
    def setUp(self):
        #TODO fetch these from a config not hard coded
        self.db = LocalDatabase('localhost','root','26Selsey', 'tap_dat_test')
        self.db.create_table('test_table', self.table_spec)
        
    def tearDown(self):
        self.db.drop_table('test_table')
        
    def test_query(self):
        self.assertEqual(self.db.query('SELECT * FROM test_table'), [])
        
    def test_insert_table(self):
        row = { 'string_column': 'Hello',
                'variable_string_column': 'There',
                'integer_column': 21,
                'smallint_column': 2,
                'bigint_column': 123456,
                'double_column': 1.23456,
                'real_column':  2.34567}
        
        self.db.insert('test_table', row)
        self.assertEqual(self.db.query('SELECT * FROM test_table'),
                         [('Hello', 'There', 21, 2, 123456, 1.23456, 2.34567)])
    
    def test_insert_multiple(self):
        rows = [
            {
                'string_column': 'Hello',
                'variable_string_column': 'There',
                'integer_column': 21,
                'smallint_column': 2,
                'bigint_column': 123456,
                'double_column': 1.23456,
                'real_column':  2.34567
            },
            {
                'string_column': 'General',
                'variable_string_column': 'Kenobi',
                'integer_column': 34,
                'smallint_column': 12,
                'bigint_column': 78901,
                'double_column': 78.901,
                'real_column':  2535.6837
            }
        ]
        
        self.db.insert_multiple('test_table', rows)
        self.assertEqual(self.db.query('SELECT * FROM test_table'),
                         [('Hello', 'There', 21, 2, 123456, 1.23456, 2.34567),
                          ('General', 'Kenobi', 34, 12, 78901, 78.901, 2535.6837)])
            
        
        