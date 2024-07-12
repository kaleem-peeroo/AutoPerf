import unittest
import warnings
import os
import random
import autoperf as ap
import pandas as pd
from icecream import ic
from typing import Dict, Optional
from datetime import datetime, timedelta

class TestAutoPerf(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

if __name__ == '__main__':
    warnings.filterwarnings("ignore", category=FutureWarning)
    unittest.main()
