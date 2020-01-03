import unittest
from tap_dat.local_database import LocalDatabase
from tap_dat.remote_database import RemoteDatabase

class TestDatabaseIntegration(unittest.TestCase):
    
    def test_table_creation_with_remote_schema(self):
        return None
        remote = RemoteDatabase('http://api.skymapper.nci.org.au/public/tap')
        local = LocalDatabase('localhost','root','26Selsey', 'tap_dat_test')
        
        remote_schema = remote.get_schema('dr1.master')
        
        local.create_table('intergration_test_table', )
        