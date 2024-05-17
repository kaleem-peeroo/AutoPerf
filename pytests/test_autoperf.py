import unittest
import warnings
import sys
import autoperf as ap
from icecream import ic

class TestAutoPerf(unittest.TestCase):
    def test_read_config(self):
        sys.argv = ['autoperf.py', './pytests/config_1.json']
        config = ap.read_config("./pytests/config_1.json")
        self.assertNotEqual(config, None)

        sys.argv = ['autoperf.py', './pytests/config_2.json']
        config = ap.read_config("./pytests/config_2.json")
        self.assertEqual(config, None)

    def test_get_difference_between_lists(self):
        self.assertEqual(
           ap.get_difference_between_lists(
            [1, 2, 3],
            [1, 2, 3]
           ),
           []
        )

        self.assertEqual(
           ap.get_difference_between_lists(
            [1, 3],
            [1, 2, 3]
           ),
           [2]
        )

        self.assertEqual(
           ap.get_difference_between_lists(
            [],
            [1, 2, 3]
           ),
           [1, 2, 3]
        )

    def test_get_longer_list(self):
        # TODO
        pass

    def test_get_shorter_list(self):
        # TODO
        pass


if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    unittest.main()
