import sys
import unittest
import data_processor as dp

from unittest.mock import patch

class TestMyCode(unittest.TestCase):
    def setUp(self):
        self.args = ['controller.json']
    
    def tearDown(self):
        self.args = None
    
    def test_get_args(self):
        with self.assertRaises(SystemExit) as cm:
            args = dp.get_args()
        self.assertEqual(cm.exception.code, 1)
        
if __name__ == '__main__':
    unittest.main()