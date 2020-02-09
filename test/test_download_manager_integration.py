import unittest

from tap_dat.download_manager import DownloadManager
from tap_dat.local_database import LocalDatabase
from tap_dat.transformer import Transformer

class TestDownloadManagerIntegration(unittest.TestCase):
    
    def setUp(self):
        self.dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id',
            0, 50, 10
        )

        self.local = LocalDatabase('localhost','root','26Selsey', 'tap_dat_test')
        self.local.create_table('dm_integration_test', Transformer.transform_schema(self.dm.get_schema()))

    def tearDown(self):
        self.local.drop_table('dm_integration_test')

    def test_single_batch_of_10_put_in_local_database(self):
        download_manager = DownloadManager('http://api.skymapper.nci.org.au/public/tap', 
                                  'dr1.master', 'object_id', 
                                  1, 10, 10)

        self.local.insert_multiple('dm_integration_test',Transformer.transform(download_manager.next()))

        result = self.local.query('SELECT object_id FROM dm_integration_test')

        for i in [i for i in range(1, 11)]:
            self.assertEqual(result[i-1][0], i)

    def test_5_batches_of_10_put_in_local_database(self):
        download_manager = DownloadManager('http://api.skymapper.nci.org.au/public/tap', 
                                  'dr1.master', 'object_id', 
                                  1, 50, 10)
        for i in range(10):
            self.local.insert_multiple('dm_integration_test', Transformer.transform(download_manager.next())) # 10 times

        results = self.local.query('SELECT object_id FROM dm_integration_test') 

        for i in [i for i in range(1, 51)]:
            self.assertEqual(results[i-1][0], i)