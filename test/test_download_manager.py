import unittest
from tap_dat.download_manager import DownloadManager
import pprint

pp = pprint.PrettyPrinter(indent=4)

class TestDownloadManager(unittest.TestCase):
    
    def test_query_queue_on_declaration(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id'
        )
        dm.set_batches(0, 50, 10)
        
        expected = [[0, 10], [10, 20], [20, 30], [30, 40], [40, 50]]
        
        self.assertEqual(dm.query_queue, expected)
     
    # find a way to test this maybe also with a fixture
    def test_next(self):
        # the first batch will be one less than all the others
        # because in this table the id starts at 1
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id'
        )
        dm.set_batches(0, 10, 10)

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
            'object_id'
        )
        dm.set_batches(0, 20, 10)

        results = dm.next()
        more_results = dm.next()
        no_more_results = dm.next()

        self.assertEqual(len(results), 10)
        self.assertEqual(results[0]['object_id'], 1)
        self.assertEqual(results[9]['object_id'], 10)

        self.assertEqual(len(more_results), 10)
        self.assertEqual(more_results[0]['object_id'], 11)
        self.assertEqual(more_results[9]['object_id'], 20)

        self.assertEqual([], no_more_results)

    def test_set_batches_when_non_divisible_batch_size(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.master',
            'object_id'
        )
        with self.assertRaises(ArithmeticError):
            dm.set_batches(0, 20, 7)

    def test_multiple_next_with_dr1_dr1p1_master(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'dr1.dr1p1_master',
            'object_id'
        )
        dm.set_batches(10, 30, 10)

        results = dm.next()
        more_results = dm.next()
        no_more_results = dm.next()

        self.assertEqual(len(results), 10)
        self.assertEqual(results[0]['object_id'], 11)
        self.assertEqual(results[9]['object_id'], 20)

        self.assertEqual(len(more_results), 10)
        self.assertEqual(more_results[0]['object_id'], 21)
        self.assertEqual(more_results[9]['object_id'], 30)

        self.assertEqual([], no_more_results)

    def test_next_with_allwise_table(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'ext.allwise',
            'raj2000'
        )
        dm.set_batches(0.0, 0.000001, 0.000001)

        results = dm.next()
        no_results = dm.next()

        self.assertEquals(len(results), 3)
        self.assertEquals(no_results, [])
        
    def test_multiple_next_with_allwise_table(self):
        dm = DownloadManager(
            'http://api.skymapper.nci.org.au/public/tap',
            'ext.allwise',
            'raj2000'
        )
        dm.set_batches(0.0, 0.000002, 0.000001)

        results = dm.next()
        more_results = dm.next()
        no_results = dm.next()

        self.assertEquals(len(results), 3)
        self.assertEqual(len(more_results), 4)
        self.assertEquals(no_results, [])