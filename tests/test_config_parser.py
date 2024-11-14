import unittest
from src.config_parser import ConfigParser

class TestConfigParser(unittest.TestCase):
    def test_init(self):
        parser = ConfigParser("test.ini")
        self.assertEqual(parser.filename, "test.ini")
        self.assertEqual(parser.config, {})

if __name__ == "__main__":
    unittest.main()
