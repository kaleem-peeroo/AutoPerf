import ast
import logging
import pandas as pd

from typing import Dict, Optional, Tuple
from rich.console import Console
console = Console()

from constants import *
from ap_config_functions import *
from ap_execution_functions import *
from ap_test_functions import *
from campaign_functions import *
from connection_functions import *
from ess_functions import *
from qos_functions import *
from utility_functions import *

def get_next_test_from_ess(ess_df: pd.DataFrame) -> Optional[Dict]:
    """
    Get the next test from the ESS dataframe by checking the last test.

    Params:
        - ess_df (pd.DataFrame): ESS dataframe.

    Returns:
        - Dict: Dictionary containing qos values for the next test.
    """
    if ess_df is None:
        logger.error(
            f"No ESS dataframe passed."
        )
        return None

    if len(ess_df.index) == 0:
        logger.error(
            f"ESS dataframe is empty."
        )
        return {}

    last_test = ess_df.iloc[-1]

    if last_test is None:
        logger.error(
            f"Couldn't get the last test."
        )
        return None

    if isinstance(last_test['qos_settings'], str):
        last_test_qos = last_test['qos_settings'].replace("'", "\"")
        last_test_qos = ast.literal_eval(last_test_qos)
    else:
        last_test_qos = last_test['qos_settings']

    return last_test_qos

def have_last_n_tests_failed(ess_df: pd.DataFrame, n: int = 10) -> Tuple[Optional[bool], Optional[str]]:
    """
    Check if the last n tests have failed from the ESS.

    Params:
        - ess_df (pd.DataFrame): ESS dataframe.
        - n (int): Number of tests to check.

    Returns:
        - bool: True if the last n tests have failed, False otherwise
        - error
    """
    if ess_df is None:
        return False, f"No ESS dataframe passed."

    if n == 0:
        return False, f"Can't check last 0 tests."

    if n < 0:
        return False, f"Can't check negative number of tests: {n}."

    if len(ess_df.index) == 0:
        return False, None

    last_n_tests = ess_df.tail(n)
    if last_n_tests is None:
        return False, f"Couldn't get the last {n} tests."

    failed_tests = last_n_tests[last_n_tests['end_status'] != 'success']
    if len(failed_tests.index) == n:
        return True, None

    return False, None

def get_expected_csv_file_count_from_test_name(test_name: str = "") -> Optional[int]:
    # TODO: Implement unit tests for this function
    """
    Get the expected number of CSV files from the test name by counting the number of SUBs and adding 1.

    Params:
        - test_name (str): Test name.

    Returns:
        - int: Expected number of CSV files if valid, None
    """

    if test_name == "":
        logger.error(
            f"No test name passed."
        )
        return None

    if test_name.strip() == "":
        logger.error(
            f"Empty test name passed."
        )
        return None

    if "SUB" not in test_name:
        logger.error(
            f"Test name does not contain 'SUB'."
        )
        return None

    sub_count_from_name = int(test_name.split("SUB_")[0].split("_")[-1])

    return sub_count_from_name + 1
