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

        parser = ConfigParser("config/test_configs/test_four.toml")
        self.assertEqual(parser.filename, "config/test_configs/test_four.toml")
        self.assertRaises(ValueError, parser.validate)

        parser = ConfigParser("config/test_configs/test_five.toml")
        self.assertEqual(parser.filename, "config/test_configs/test_five.toml")
        self.assertRaises(ValueError, parser.validate)

        parser = ConfigParser("config/test_configs/test_six.toml")
        self.assertEqual(parser.filename, "config/test_configs/test_six.toml")
        self.assertRaises(ValueError, parser.validate)

        parser = ConfigParser("config/test_configs/test_seven.toml")
        self.assertEqual(parser.filename, "config/test_configs/test_seven.toml")
        self.assertRaises(ValueError, parser.validate)


if __name__ == "__main__":
    unittest.main()
