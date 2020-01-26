import unittest
from tap_dat.local_database import LocalDatabase
from tap_dat.remote_database import RemoteDatabase
from tap_dat.transformer import Transformer
import pprint

class TestDatabaseIntegration(unittest.TestCase):

    def setUp(self):
        self.remote = RemoteDatabase('http://api.skymapper.nci.org.au/public/tap')
        self.local = LocalDatabase('localhost','root','26Selsey', 'tap_dat_test')

    def tearDown(self):
        self.local.drop_table('integration_test_table')
        return None # remove this later

    def test_table_creation_with_remote_schema(self):
        remote_schema = self.remote.get_schema('dr1.master', "column_name,datatype")
        # maybe also test with a fixture
        self.local.create_table('integration_test_table', remote_schema)

    def test_table_creation_and_single_insertion_of_remote_tuple(self):

        transformed_schema = Transformer.transform_schema(self.remote.get_schema('dr1.master', "column_name,datatype,size"))

        rows = Transformer.transform(self.remote.query("SELECT * FROM dr1.master WHERE object_id=1"))
        row = rows[0]

        self.local.create_table('integration_test_table', transformed_schema)
        self.local.insert('integration_test_table', row)
        
        