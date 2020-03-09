
import unittest
from tap_dat.controller import Controller


class TestController(unittest.TestCase):

    def test__init__(self):
        self.assertRaises(Exception, Controller())

    def test_setup_local_connection_when_remote_not_setup(self):
        with self.assertRaises(AttributeError):
            Controller.get_instance().setup_local_connection(
                'a_host',
                'user',
                'pass',
                'db_name',
                'table_name'
            )
    
    def test_setup_remote_connection(self):
        with self.assertRaises(Exception):
            Controller.get_instance().setup_remote_connection(
                'url',
                'remote_table',
                'table_id'
            )
        
        
