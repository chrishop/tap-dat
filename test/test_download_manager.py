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
            1, 50, 10
        )
        
        expected = [[1, 10], [11, 20], [21, 30], [31, 40], [41, 50]]
        
        self.assertEqual(dm.query_queue, expected)
     
    # find a way to test this maybe also with a fixture
    def test_next(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id',
            1, 10, 10
        )

        result = dm.next()
        no_more_results = dm.next()

        self.assertEqual(len(result), 10)
        self.assertEqual(result[0]['object_id'], 1)
        self.assertEqual(result[9]['object_id'], 10)

        self.assertEqual([], no_more_results)

    def test_multiple_next(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id',
            1, 20, 10
        )

        results = dm.next()
        more_results = dm.next()
        no_more_results = dm.next()

        self.assertEqual(len(results), 10)
        self.assertEqual(results[0]['object_id'], 1)
        self.assertEqual(results[9]['object_id'], 10)

        self.assertEqual(len(more_results), 10)
        self.assertEqual(more_results[0]['object_id'], 11)
        self.assertEqual(more_results[9]['object_id'], 20)

        self.assertEqual(len(more_results), 10)
        self.assertEqual([], no_more_results)