import unittest
from tap_dat.download_manager import DownloadManager
import pprint

pp = pprint.PrettyPrinter(indent=4)

class TestDownloadManager(unittest.TestCase):
    
    def test_query_queue_on_declaration(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id',
            0, 50, 10
        )
        
        expected = [[0, 10], [10, 20], [20, 30], [30, 40], [40, 50]]
        
        self.assertEqual(dm.query_queue, expected)
     
    # find a way to test this maybe also with a fixture
    def test_next(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id',
            0, 50, 10
        )
        
        