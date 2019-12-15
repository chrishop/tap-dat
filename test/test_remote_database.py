import unittest
from tap_dat.remote_database import RemoteDatabase
import numpy as np

class TestRemoteDatabase(unittest.TestCase):
    
    # more of an intergration test

    def setUp(self):
        self.skymapper = RemoteDatabase('http://api.skymapper.nci.org.au/public/tap')

    def test_query_with_table_data(self):
        result = self.skymapper.query('SELECT object_id, raj2000, dej2000, apass_dist \
                                  FROM dr1.master WHERE object_id=1')

        expected = [{ 'object_id': 1,
                      'raj2000': 315.001047, 
                      'dej2000': -35.252904, 
                      'apass_dist': 'NULL'}]

        self.assertEqual(result,expected)

    def test_query_with_schema_data(self):
        result = self.skymapper.query("SELECT column_name, column_order, description, unit \
                                    FROM TAP_SCHEMA.columns \
                                    WHERE table_name='dr1.master' \
                                    AND column_name='object_id' \
                                    ORDER BY column_order")

        expected = [{ 'column_name': 'object_id',
                      'column_order': 1,
                      'description': 'Global unique object ID in the master table.',
                      'unit': 'NULL'}]

        self.assertEqual(result,expected)

    # find a better way of testing this (maybe with fixtures)
    # maybe change the function
    def test_get_schema(self):
        result = self.skymapper.get_schema('dr1.master')

    def test_get_batch(self):
        result = self.skymapper.get_batch('dr1.master', 'object_id', 1, 11, 'object_id')

        expected = [    {'object_id': 1},
                        {'object_id': 2},
                        {'object_id': 3},
                        {'object_id': 4},
                        {'object_id': 5},
                        {'object_id': 6},
                        {'object_id': 7},
                        {'object_id': 8},
                        {'object_id': 9},
                        {'object_id': 10}]

        self.assertEqual(result, expected)
