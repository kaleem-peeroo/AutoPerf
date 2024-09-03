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
import shlex
import socket
import smtplib
import pandas as pd

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from my_secrets import APP_PASSWORD
from icecream import ic
from typing import Dict, List, Optional, Tuple
from pprint import pprint
from multiprocessing import Process, Manager
from rich.progress import track
from rich.console import Console
console = Console()

warnings.simplefilter(action='ignore', category=FutureWarning)

from constants import *
from ap_config_functions import *
from ap_execution_functions import *
from ap_test_functions import *
from campaign_functions import *
from connection_functions import *
from ess_functions import *
from qos_functions import *
from utility_functions import *

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, 
    filename="logs/autoperf.log", 
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

def main(sys_args: list[str] = []) -> Optional[None]:
    if len(sys_args) < 2:
        logger.error(
            f"Config filepath not specified."
        )
        return 

    # Make all the output folders:
    # - /ess
    # - /data
    # - /summarised_data
    # - /datasets

    os.makedirs("output", exist_ok=True)
    os.makedirs(ESS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SUMMARISED_DIR, exist_ok=True)
    os.makedirs(DATASET_DIR, exist_ok=True)

    CONFIG_PATH = sys_args[1]
    logger.info(f"Reading config: {CONFIG_PATH}", )
    CONFIG, config_error = read_config(CONFIG_PATH)
    if config_error:
        logger.error(f"Error reading config: {config_error}")
        return
    logger.info(f"Config read: {CONFIG_PATH}", )

    for EXPERIMENT_INDEX, EXPERIMENT in enumerate(CONFIG):
        EXPERIMENT_NAME = EXPERIMENT['experiment_name']

        # logger.debug(f"[{EXPERIMENT_INDEX + 1}/{len(CONFIG)}] Running {EXPERIMENT['experiment_name']}...")
        logger.info(
            f"[{EXPERIMENT_INDEX + 1}/{len(CONFIG)}] Running {EXPERIMENT['experiment_name']}..."
        )

        EXPERIMENT_DIRPATH, dirname_error = get_dirname_from_experiment(EXPERIMENT)
        if dirname_error:
            logger.error(f"Error getting experiment dirname: {dirname_error}")
            continue
        os.makedirs(EXPERIMENT_DIRPATH, exist_ok=True)
        logger.info(f"Created {EXPERIMENT_DIRPATH}", )

        is_pcg, if_pcg_error = get_if_pcg(EXPERIMENT)
        if if_pcg_error:
            logger.error(f"Error check if PCG: {if_pcg_error}")
            continue 

        EXPERIMENT_DIRNAME = os.path.basename(EXPERIMENT_DIRPATH)
        ESS_PATH = os.path.join(ESS_DIR, f"{EXPERIMENT_DIRNAME}.csv")
        ess_df, ess_error = get_ess_df(ESS_PATH)
        if ess_error:
            logger.error(f"Error getting ess: {ess_error}")
            continue

        if is_pcg:
            COMBINATIONS, combinations_error = generate_combinations_from_qos(
                EXPERIMENT['qos_settings']
            )
            if combinations_error:
                logger.error(f"Error generating combinations: {combinations_error}")
                continue
            target_test_count = len(COMBINATIONS)
        else:
            target_test_count = EXPERIMENT['rcg_target_test_count']

        ESS_DF_ROW_COUNT = len(ess_df.index)

        current_test_index = ESS_DF_ROW_COUNT
        last_test_index = target_test_count

        for test_index in range(current_test_index, last_test_index):
            current_test_index_string = test_index + 1
            target_test_count_string = target_test_count
            counter_string = f"[{current_test_index_string}/{target_test_count_string}]"

            ess_df, ess_error = get_ess_df(ESS_PATH)
            if ess_error:
                logger.error(f"Error getting ess: {ess_error}")
                continue

            must_wait_for_self_reboot, must_self_reboot_error = get_must_wait_for_self_reboot(ess_df)
            if must_self_reboot_error:
                logger.warning(
                    f"Couldn't check if self-reboot needed: {must_self_reboot_error}"
                )

            if must_wait_for_self_reboot:
                ip_output, ip_emoji_dict, ip_error = get_ip_output_from_ess_df(ess_df)
                if ip_error:
                    logger.warning(f"Error getting ip output to send email: {ip_error}")

                emoji_output = ""
                for ip, emoji in ip_emoji_dict.items():
                    emoji_output += f"{ip}: {emoji}\n"

                logger.info("Sending email to notify of self-reboot...")
                send_email(
                    "AutoPerf: Reboot Notification"
                    f"{EXPERIMENT_NAME}\n {ip_output} {emoji_output}"
                )

                logger.warning(f"""
Last few tests have failed because of the same machine being unreachable.
Waiting 2 minutes for the machine to self-reboot.
                """)
                time.sleep(120)

            if is_pcg:
                test_config = COMBINATIONS[test_index]
            else:
                test_config = generate_test_config_from_qos(EXPERIMENT['qos_settings'])
                if test_config is None:
                    logger.error("Error generating RCG config")
                    continue

            test_name, test_name_error = get_test_name_from_combination_dict(test_config)
            if test_name_error:
                logger.error(f"Error getting test name: {test_name_error}")
                continue

            logger.info(
                f"{counter_string} [{test_name}] Running test..."
            )

            ess_df, run_test_error = run_test(
                test_config,
                EXPERIMENT['slave_machines'],
                ess_df,
                EXPERIMENT_DIRPATH,
                EXPERIMENT['noise_generation']
            )

            ess_df.to_csv(ESS_PATH, index = False)

            if run_test_error:
                logger.error(f"Error running test {test_name}: {run_test_error}")
                logger.info(
                    f"[{EXPERIMENT_NAME}] {counter_string} [{test_name}] failed."
                )
                continue
        
        do_ess_rows_match_test_count, func_error = check_if_ess_rows_match_expected_test_count(
            EXPERIMENT
        )
        if func_error:
            logger.error(f"""
                {func_error}
                Error checking if ESS row count matches expected test count for {EXPERIMENT_NAME}.
                Skipping post test data processing...""")
            continue

        if not do_ess_rows_match_test_count:
            logger.warning(f"""
                ESS rows don't match expected test count for {EXPERIMENT_NAME}. 
                Skipping post test data processing...""")
            continue

        # TODO: Retry failed tests here
        """
        for failed_test in failed_tests:
            retry_counter = 3
            test_status = "failed"

            while retry_counter > 0 and test_status != "success":
                ess_df, run_test_error = run_test(
                    failed_test,
                    EXPERIMENT['slave_machines'],
                    ess_df,
                    EXPERIMENT_DIRPATH,
                    EXPERIMENT['noise_generation']
                )

                if run_test_error:
                    logger.error(f"Error running test {test_name}: {run_test_error}")
                    logger.info(
                        f"[{EXPERIMENT_NAME}] {counter_string} [{test_name}] failed."
                    )
                    continue

                test_status = "success"
                retry_counter -= 1
        """
            
        # Do a check on all tests to make sure expected number of pub and sub files are the same
        test_dirpaths = [os.path.join(EXPERIMENT_DIRPATH, _) for _ in os.listdir(EXPERIMENT_DIRPATH)]
        test_dirpaths = [_ for _ in test_dirpaths if os.path.isdir(_)]
        test_dirpaths = [_ for _ in test_dirpaths if "data" not in _.lower()]
        for test_dirpath in test_dirpaths:
            test_name = os.path.basename(test_dirpath)
            expected_csv_file_count = get_expected_csv_file_count_from_test_name(test_name)
            actual_csv_file_count = get_csv_file_count_from_dir(test_dirpath)

            if expected_csv_file_count != actual_csv_file_count:
                logger.warning(
                    f"{test_name} has {actual_csv_file_count} .csv files instead of {expected_csv_file_count}"
                )

        # Summarise tests
        summarise_tests(EXPERIMENT_DIRPATH) 
        if summarise_tests is None:
            logger.error(
                f"Error summarising tests for {EXPERIMENT['experiment_name']}"
            )
            continue

        truncation_percentages = [0, 10, 25, 50]
        for truncation_percent in truncation_percentages:
            trunc_ds_path = generate_dataset(EXPERIMENT_DIRPATH, truncation_percent)
            if trunc_ds_path is None:
                logger.error(
                    f"Error generating dataset for {EXPERIMENT['experiment_name']} with {truncation_percent}% truncation."
                )
                continue

            # logger.info(f"Generated dataset with {truncation_percent}% truncation:\n\t{trunc_ds_path}")

        # Compress results at end of experiment
        if os.path.exists(f"{EXPERIMENT_DIRPATH}.zip"):
            logger.warning(f"A compressed version of the results already exists...")

        else:
            logger.info(f"Compressing {EXPERIMENT_DIRPATH} to {EXPERIMENT_DIRPATH}.zip...")
            shutil.make_archive(
                EXPERIMENT_DIRPATH,
                'zip',
                EXPERIMENT_DIRPATH
            )

if __name__ == "__main__":
    main(sys.argv)

    # if pytest.main(["-q", "./pytests/test_autoperf.py", "--exitfirst"]) == 0:
        # main(sys.argv)
    # else:
        # logger.error("Tests failed.")
        # sys.exit(1)
