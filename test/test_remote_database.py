import unittest
from astropy.table.table_helpers import simple_table
from tap_dat.remote_database.remote_database import RemoteDatabase

class TestRemoteDatabase(unittest.TestCase):
    
    def test_table_to_dict(self):
        table = simple_table()
        
        expected = [{'a': 1, 'b': 1.0, 'c': 'c'},
                    {'a': 2, 'b': 2.0, 'c': 'd'},
                    {'a': 3, 'b': 3.0, 'c': 'e'}]
        
        self.assertEqual(RemoteDatabase.table_to_dict(table), expected)