import subprocess
import sys
import time
import ast
import itertools
import re
import pytest
import os
import logging
import json
import datetime
import warnings
import random
import shutil
import rich

from icecream import ic
from typing import Dict, List, Optional
from pprint import pprint
from multiprocessing import Process, Manager
from rich.progress import track

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd

DEBUG_MODE = False
SKIP_RESTART = False

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, 
    filename="autoperf_results_salvager.log", 
    filemode="w",
    format='%(asctime)s \t%(levelname)s \t%(message)s'
)
logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
if DEBUG_MODE:
    console_handler.setLevel(logging.DEBUG)
else:
    console_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s \t%(levelname)s \t%(message)s'
)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

CONFIG = [
    {
        "name": "Multicast Exploration 100B",
        "paths": [
            "summarised_data/3pi/Multicast_Exploration",
            "summarised_data/3pi/Multicast_Exploration_Run_2",
            "summarised_data/3pi/Multicast_Exploration_Run_3",
        ]
    }
]

def validate_experiment(experiment: Dict) -> bool:
    if experiment.keys() != {"name", "paths"}:
        logger.error("Experiment does not have the correct keys.")
        return False

    if experiment["name"] == "":
        logger.error("Experiment name is empty.")
        return False

    if type(experiment["paths"]) != list:
        logger.error(f"Experiment paths is not a list of strings:\n\t{'\n\t'.join(experiment['paths'])}")
        return False

    if experiment["paths"] == []:
        logger.error("Experiment paths are empty.")
        return False

    if len(experiment["paths"]) == 1:
        logger.error(f"Only one path is provided: {experiment['paths']}")
        return False

    for path in experiment["paths"]:
        if not os.path.exists(path):
            logger.error(f"Path does not exist: {path}")
            return False

        if not os.path.isdir(path):
            logger.error(f"Path is not a directory: {path}")
            return False

        if not os.listdir(path):
            logger.error(f"Path is empty: {path}")
            return False

        if not any(path.endswith(".csv") for path in os.listdir(path)):
            logger.error(f"Path does not contain any csv files: {path}")

    return True

def main(argv):

    for EXPERIMENT in CONFIG:
        """
        1. Validate name and paths are populated
        2. Make sure there is more than 1 path
        3. For each path, check if the path exists and if contains csv files
        4. Collect filepaths from all folders.
        5. Remove duplicate basenames.
        6. Combine all files into a single folder.
        7. Generate a dataset from the new folder.
        """

        if not validate_experiment(EXPERIMENT):
            logger.error("Experiment is not valid.")
            continue

if __name__ == "__main__":
    if pytest.main(["-q", "./pytests/test_autoperf_results_salvager.py", "--exitfirst"]) == 0:
        main(sys.argv)
    else:
        logger.error("Tests failed.")
        sys.exit(1)
