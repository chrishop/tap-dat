import unittest
from tap_dat.remote_database import RemoteDatabase
import numpy as np

class TestRemoteDatabase(unittest.TestCase):
    
    # more of an intergration test
    def test_query_with_table_data(self):
        skymapper = RemoteDatabase('http://api.skymapper.nci.org.au/public/tap')
        
        result = skymapper.query('SELECT object_id, raj2000, dej2000, apass_dist \
                                  FROM dr1.master WHERE object_id=1')
        
        expected = [{ 'object_id': 1,
                      'raj2000': 315.001047, 
                      'dej2000': -35.252904, 
                      'apass_dist': 'NULL'}]
        
        self.assertEqual(result,expected)
        
    def test_query_with_schema_data(self):
        skymapper = RemoteDatabase('http://api.skymapper.nci.org.au/public/tap')
        
        result = skymapper.query("SELECT column_name, column_order, description, unit \
                                    FROM TAP_SCHEMA.columns \
                                    WHERE table_name='dr1.master' \
                                    AND column_name='object_id' \
                                    ORDER BY column_order")
        
        expected = [{ 'column_name': 'object_id',
                      'column_order': 1,
                      'description': 'Global unique object ID in the master table.',
                      'unit': 'NULL'}]
        
        self.assertEqual(result,expected)