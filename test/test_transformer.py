import unittest
from tap_dat.transformer import Transformer
from tap_dat.remote_database import RemoteDatabase


class TestTransformer(unittest.TestCase):
        
    def test_transform(self):
        result = RemoteDatabase('http://api.skymapper.nci.org.au/public/tap').query(
            'SELECT object_id, raj2000, dej2000, apass_dist FROM dr1.master WHERE object_id=1'
        )
        
        expected = [{
            'object_id': '1',
            'raj2000': '315.001047', 
            'dej2000': '-35.252904'
            # apass_dist should be null and so removed from the dict
        }]
        
        transformed = Transformer.transform(result)
        self.assertRaises(KeyError, lambda: transformed[0]['apass_dist'])
        self.assertEqual(expected, transformed)
        
    def test_schema_tranform_when_char_is_below_255(self):
        from_remote = [{'datatype': 'CHAR', 'size': 50}]
        
        expected = [{'datatype': 'CHAR(50)', 'size': 50}]
        self.assertEqual(Transformer.transform_schema(from_remote), expected)
        
    def test_schema_tranform_when_char_is_above_255(self):
        from_remote = [{'datatype': 'CHAR', 'size': 300}]
        
        expected = [{'datatype': 'CHAR(255)', 'size': 300}]
        self.assertEqual(Transformer.transform_schema(from_remote), expected)
        
    def test_schema_tranform_when_datatype_is_not_char(self):
        from_remote = [{'datatype': 'DOUBLE', 'size': 8}]
        
        expected = [{'datatype': 'DOUBLE', 'size': 8}]
        self.assertEqual(Transformer.transform_schema(from_remote), expected)
            
        