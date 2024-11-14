import unittest
from src.config_parser import ConfigParser

class TestConfigParser(unittest.TestCase):
    def test_init(self):
        parser = ConfigParser("config/test_configs/test_one.toml")
        self.assertEqual(parser.filename, "config/test_configs/test_one.toml")
        self.assertEqual(parser.config, {})
        self.assertRaises(ValueError, parser.parse)

        parser = ConfigParser("config/test_configs/test_two.toml")
        self.assertEqual(parser.filename, "config/test_configs/test_two.toml")
        parser.parse()
        self.assertEqual(len(parser.config), 1)

if __name__ == "__main__":
    unittest.main()
