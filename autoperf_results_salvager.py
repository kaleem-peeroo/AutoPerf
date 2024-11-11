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
from rich.console import Console

console = Console()

warnings.simplefilter(action="ignore", category=FutureWarning)

import pandas as pd

DEBUG_MODE = False
SKIP_RESTART = False

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    filename="autoperf_results_salvager.log",
    filemode="w",
    format="%(asctime)s \t%(levelname)s \t%(message)s",
)
logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
if DEBUG_MODE:
    console_handler.setLevel(logging.DEBUG)
else:
    console_handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s \t%(levelname)s \t%(message)s")
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

PERCENTILES = [
    0,
    1,
    2,
    3,
    4,
    5,
    10,
    20,
    30,
    40,
    60,
    70,
    80,
    90,
    95,
    96,
    97,
    98,
    99,
    100,
    25,
    50,
    75,
]

REQUIRED_QOS_KEYS = [
    "datalen_bytes",
    "durability_level",
    "duration_secs",
    "latency_count",
    "pub_count",
    "sub_count",
    "use_multicast",
    "use_reliable",
]

CONFIG = [
    {
        "name": "Multicast Exploration 100B",
        "paths": [
            "summarised_data/3pi/Multicast_Exploration",
            "summarised_data/3pi/Multicast_Exploration_Run_2",
            "summarised_data/3pi/Multicast_Exploration_Run_3",
        ],
    }
]


def get_qos_dict_from_test_name(test_name: str = "") -> Optional[Dict]:
    """
    Take test name and get qos settings from it.
    e.g. 100SEC_100B_5PUB_1SUB_REL_MC_0DUR_100LC returns:
    {
        "datalen_byts": 100,
        "durability_level": 0,
        "duration_secs": 100,
        "latency_count": 100,
        "pub_count": 5,
        "sub_count": 1,
        "use_multicast": true,
        "use_reliable": true
    }
    """
    if test_name == "":
        logger.error("No test name passed to get_qos_dict_from_test_name().")
        return None

    # TODO: Write unit tests for this function

    qos_dict = {
        "datalen_bytes": None,
        "durability_level": None,
        "duration_secs": None,
        "latency_count": None,
        "pub_count": None,
        "sub_count": None,
        "use_multicast": None,
        "use_reliable": None,
    }

    if "." in test_name:
        test_name = test_name.split(".")[0]

    if "_" not in test_name:
        logger.error("No _ found in test name: {test_name}")
        return None

    test_name_sections = test_name.split("_")

    if len(test_name_sections) != len(REQUIRED_QOS_KEYS):
        logger.error(
            f"Mismatch in test_name sections. Expected {len(REQUIRED_QOS_KEYS)} parts."
        )
        return None

    for section in test_name_sections:
        if "sec" in section.lower():
            value = section.lower().replace("sec", "")
            value = int(value)
            qos_dict["duration_secs"] = value

        elif "pub" in section.lower():
            value = section.lower().replace("pub", "")
            value = int(value)
            qos_dict["pub_count"] = value

        elif "sub" in section.lower():
            value = section.lower().replace("sub", "")
            value = int(value)
            qos_dict["sub_count"] = value

        elif "dur" in section.lower():
            value = section.lower().replace("dur", "")
            value = int(value)
            qos_dict["durability_level"] = value

        elif "lc" in section.lower():
            value = section.lower().replace("lc", "")
            value = int(value)
            qos_dict["latency_count"] = value

        elif "rel" in section.lower() or "be" in section.lower():
            use_reliable = None
            if "rel" in section.lower():
                use_reliable = True
            else:
                use_reliable = False

            qos_dict["use_reliable"] = use_reliable

        elif "mc" in section.lower() or "uc" in section.lower():
            use_multicast = None

            if "mc" in section.lower():
                use_multicast = True
            else:
                use_multicast = False

            qos_dict["use_multicast"] = use_reliable

        elif section.lower().endswith("b"):
            value = section.lower().replace("b", "")
            value = int(value)
            qos_dict["datalen_bytes"] = value

        else:
            logger.error(f"Couldn't recognise following section: {section}")
            return None

    # Final check for any None values
    for key, value in qos_dict.items():
        if value is None:
            logger.error(f"Value for {key} is None.")
            return None

    return qos_dict


def generate_dataset(
    summaries_dirpath: str = "", truncation_percent: int = 0
) -> Optional[str]:
    """
    Reduce each csv file in summarised_data folder to a single row in one csv file.

    Params:
        - dirpath (str): Directory path.
        - truncation_percent (int): Truncation percent.

    Returns:
        - None
    """
    if summaries_dirpath == "":
        logger.error(f"No summaries dirpath passed.")
        return None

    if not os.path.exists(summaries_dirpath):
        logger.error(f"Summarised dirpath {summaries_dirpath} does NOT exist.")
        return None

    test_csvs = [
        os.path.join(summaries_dirpath, _) for _ in os.listdir(summaries_dirpath)
    ]
    test_csvs = [_ for _ in test_csvs if _.endswith(".csv")]
    if len(test_csvs) == 0:
        logger.error(f"No csv files found in {summaries_dirpath}.")
        return None

    logger.info(
        f"Generating dataset from {len(test_csvs)} tests with {truncation_percent}% truncation..."
    )

    experiment_name = os.path.basename(summaries_dirpath)
    current_timestamp = datetime.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{current_timestamp}_{experiment_name}_dataset_{truncation_percent}_percent_truncation.csv"
    filename = os.path.join(os.path.dirname(summaries_dirpath), filename).replace(
        "summarised_data", "datasets"
    )

    os.makedirs("datasets", exist_ok=True)

    dataset_df = pd.DataFrame()
    for test_csv in track(test_csvs, description="Processing..."):
        new_dataset_row = {}

        try:
            test_df = pd.read_csv(test_csv)
        except UnicodeDecodeError:
            logger.error(f"UnicodeDecodeError reading {test_csv}.")
            continue

        test_name = os.path.basename(test_csv)

        new_dataset_row = get_qos_dict_from_test_name(test_name)
        if new_dataset_row is None:
            logger.error(f"Couldn't get qos dict for {test_name}.")
            continue

        for key, value in new_dataset_row.items():
            if value == True:
                new_dataset_row[key] = 1
            elif value == False:
                new_dataset_row[key] = 0

        for column in test_df.columns:
            if "index" in column.lower():
                continue

            column_values = test_df[column]
            value_count = len(column_values)

            values_to_truncate = int(value_count * (truncation_percent / 100))

            column_values = test_df[column].iloc[values_to_truncate:]
            column_df = pd.DataFrame(column_values)

            for PERCENTILE in PERCENTILES:
                new_dataset_row[f"{column}_{PERCENTILE}%"] = column_df.quantile(
                    PERCENTILE / 100
                ).values[0]

        new_dataset_row_df = pd.DataFrame([new_dataset_row])

        dataset_df = pd.concat(
            [dataset_df, new_dataset_row_df], axis=0, ignore_index=True
        )

    dataset_df.to_csv(filename, index=False)
    return filename


def validate_experiment(experiment: Dict) -> bool:
    if experiment.keys() != {"name", "paths"}:
        logger.error("Experiment does not have the correct keys.")
        return False

    if experiment["name"] == "":
        logger.error("Experiment name is empty.")
        return False

    if type(experiment["paths"]) != list:
        logger.error(
            f"Experiment paths is not a list of strings:\n\t{'\n\t'.join(experiment['paths'])}"
        )
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


def main():
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

        # Collect filepaths from all folders
        all_filepaths = []
        for path in EXPERIMENT["paths"]:
            all_filepaths += [
                os.path.join(path, file)
                for file in os.listdir(path)
                if file.endswith(".csv")
            ]

        # Remove duplicate basenames
        basenames = set()
        unique_filepaths = []
        for filepath in all_filepaths:
            basename = os.path.basename(filepath)
            if basename not in basenames:
                basenames.add(basename)
                unique_filepaths.append(filepath)
            else:
                logger.warning(f"Duplicate file found: {basename}")

        # Combine all files into a single folder
        combined_folder = os.path.join(
            os.path.dirname(EXPERIMENT["paths"][0]), f"salvaged_{EXPERIMENT['name']}"
        )
        os.makedirs(combined_folder, exist_ok=True)
        for filepath in track(
            unique_filepaths, description=f"Combining files for {EXPERIMENT['name']}"
        ):
            shutil.copy(filepath, combined_folder)

        console.print(
            f"Salvaged {len(unique_filepaths)} files for {EXPERIMENT['name']} to {combined_folder}.",
            style="bold green",
        )

        # Generate a dataset from the new folder
        truncation_percentages = [0, 10, 25, 50]
        for truncation_percent in truncation_percentages:
            trunc_ds_path = generate_dataset(combined_folder, truncation_percent)
            if trunc_ds_path is None:
                logger.error(
                    f"Error generating dataset for {EXPERIMENT['name']} with {truncation_percent}% truncation."
                )
                continue

            logger.info(
                f"Generated dataset with {truncation_percent}% truncation:\n\t{trunc_ds_path}"
            )


if __name__ == "__main__":
    if (
        pytest.main(
            ["-q", "./pytests/test_autoperf_results_salvager.py", "--exitfirst"]
        )
        == 0
    ):
        main()
    else:
        logger.error("Tests failed.")
        sys.exit(1)
