import unittest
import autoperf as ap
from icecream import ic

class TestAutoPerf(unittest.TestCase):
    def test_read_config(self):
        config = ap.read_config("./pytests/config_1.json")
        self.assertNotEqual(config, None)

        config = ap.read_config("./pytests/config_2.json")
        self.assertEqual(config, None)


if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    unittest.main()
