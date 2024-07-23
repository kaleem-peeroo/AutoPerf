import unittest
import warnings
import os
import random
import autoperf_results_salvager as ap
import pandas as pd
from icecream import ic
from typing import Dict, Optional
from datetime import datetime, timedelta

class TestAutoPerf(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_validate_experiment(self):
        fail_test_cases = [
            {
                "name": "",
                "paths": []
            },
            {
                "name": "test",
                "paths": []
            },
            {
                "name": "",
                "paths": ["test"]
            },
            {
                "name": "test",
                "paths": ["test"]
            },
            {
                "name": "test",
                "paths": ["test", "test"]
            }
        ]

        for i, test_case in enumerate(fail_test_cases):
            with self.subTest(i=i):
                self.assertFalse(ap.validate_experiment(test_case))

if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    unittest.main()
