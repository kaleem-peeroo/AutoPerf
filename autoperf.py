import pandas as pd
import sys
import re
import pytest
import os
import logging
import json
from icecream import ic
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, 
    filename="autoperf.log", 
    filemode="w",
    format='%(asctime)s \t%(levelname)s \t%(message)s'
)
logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s \t%(levelname)s \t%(message)s'
)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

REQUIRED_EXPERIMENT_KEYS = [
    'experiment_name',
    'combination_generation_type',
    'resuming_test_name',
    'qos_settings',
    'slave_machines'
]

REQUIRED_QOS_KEYS = [
    "datalen_bytes",
    'durability_level',
    'duration_secs',
    'latency_count',
    'pub_count',
    'sub_count',
    'use_multicast',
    'use_reliable'
]

REQUIRED_SLAVE_MACHINE_KEYS = [
    'ip',
    'machine_name',
    'participant_allocation',
    'perftest_exec_path',
    'ssh_key_path',
    'username'
]

def get_difference_between_lists(list_one: List = [], list_two: List = []):
    if list_one is None:
        logger.error(
            f"List one is none."
        )
        return None

    if list_two is None:
        logger.error(
            f"List two is none."
        )
        return None

    longer_list = get_longer_list(
        list_one, 
        list_two
    )
    if longer_list is None:
        logger.error(
            f"Couldn't get longer list"
        )
        return None

    shorter_list = get_shorter_list(
        list_one, 
        list_two
    )
    if shorter_list is None:
        logger.error(
            f"Couldn't get shorter list"
        )
        return None

    return [item for item in longer_list if item not in shorter_list]

def get_longer_list(list_one: List = [], list_two: List = []):
    if list_one is None:
        logger.error(
            f"List one is none."
        )
        return None

    if list_two is None:
        logger.error(
            f"List two is none."
        )
        return None

    if len(list_one) > len(list_two):
        return list_one
    else:
        return list_two

def get_shorter_list(list_one: List = [], list_two: List = []):
    if list_one is None:
        logger.error(
            f"List one is none."
        )
        return None

    if list_two is None:
        logger.error(
            f"List two is none."
        )
        return None

    if len(list_one) > len(list_two):
        return list_two
    else:
        return list_one

def validate_dict_using_keys(given_keys: List = [], required_keys: List = []) -> Optional[bool]:
    if given_keys == []:
        logger.error(
            f"No given_keys given."
        )
        return None

    if required_keys == []:
        logger.error(
            f"No required_keys given."
        )
        return None

    list_difference = get_difference_between_lists(
        list(given_keys), 
        required_keys
    )
    if list_difference is None:
        logger.error(
            f"Error comparing keys for {given_keys}"
        )
        return None

    if len(list_difference) > 0:
        logger.error(
            f"Mismatch in keys for \n\t{given_keys}: \n\t\t{list_difference}"
        )
        return False
    
    return True

def read_config(config_path: str = ""):
    if config_path == "":
        logger.error(
            f"No config path passed to read_config()"
        )
        return None

    with open(config_path, 'r') as f:
        try:
            config = json.load(f)
        except ValueError:
            logger.error(
                f"Error parsing JSON for config file: {config_path}"
            )
            return None

    if not isinstance(config, list):
        logger.error(
            f"Config file does not contain a list: {config_path}"
        )
        return None

    for experiment in config:
        if not isinstance(experiment, dict):
            logger.error(
                f"{experiment} is NOT a dictionary."
            )
            return None
    
        is_experiment_config_valid = validate_dict_using_keys(
            list(experiment.keys()),
            REQUIRED_EXPERIMENT_KEYS
        )
        if is_experiment_config_valid is None:
            logger.error(
                f"Error validating {experiment}."
            )
            return None
        if not is_experiment_config_valid:
            logger.error(
                f"Config invalid for {experiment}."
            )
            return None

        qos_settings = experiment['qos_settings']
        is_qos_config_valid = validate_dict_using_keys(
            list(qos_settings.keys()),
            REQUIRED_QOS_KEYS
        )
        if is_qos_config_valid is None:
            logger.error(
                f"Error validating {experiment}."
            )
            return None
        if not is_qos_config_valid:
            logger.error(
                f"Config invalid for {experiment}."
            )
            return None

        slave_machine_settings = experiment['slave_machines']
        for machine_setting in slave_machine_settings:
            is_slave_machine_config_valid = validate_dict_using_keys(
                list(machine_setting.keys()),
                REQUIRED_SLAVE_MACHINE_KEYS
            )
            if is_slave_machine_config_valid is None:
                logger.error(
                    f"Error validating slave machine {machine_setting['machine_name']} for {experiment['experiment_name']}."
                )
                return None
            if not is_slave_machine_config_valid:
                logger.error(
                    f"Config invalid for slave machine {machine_setting['machine_name']} for {experiment['experiment_name']}."
                )
                return None

    return config

def get_if_pcg(experiment: Optional[Dict] = None) -> Optional[bool]:
    if experiment is None:
        logger.error(
            f"No experiment given."
        )
        return None

    if 'combination_generation_type' not in experiment.keys():
        logger.error(
            f"combination_generation_type option not found in experiment config."
        )
        return None

    if experiment['combination_generation_type'] == "":
        logger.error(
            "combination_generation_type is empty."
        )
        return None

    combination_generation_type = experiment['combination_generation_type']
    if combination_generation_type not in ['pcg', 'rcg']:
        logger.error(
            f"Invalid value for combination generation type: {combination_generation_type}.\n\tExpected either PCG or RCG."
        )
        return None
    
    return experiment['combination_generation_type'] == 'pcg'

def get_valid_dirname(dir_name: str = "") -> Optional[str]:
    if dir_name == "":
        logger.error(
            f"No dirname passed for validation."
        )
        return None

    dir_name = re.sub(r'[<>:"/\\|?*]', '_', dir_name)
    dir_name = dir_name.strip()
    dir_name = re.sub(r'\s+', '_', dir_name)

    if not dir_name:
        logger.error(
            f"Dirname can't be empty after validation."
        )
        return None

    if len(dir_name) > 255:
        logger.error(
            f"Dirname can't be more than 255 characters:\n\t{dir_name}"
        )
        return None

    return dir_name

def get_dirname_from_experiment(experiment: Optional[Dict] = None) -> Optional[str]:
    if experiment is None:
        logger.error(
            f"No experiment config passed."
        )
        return None

    experiment_name = experiment['experiment_name']
    experiment_dirname = get_valid_dirname(experiment_name)

    return experiment_dirname

def generate_combinations_from_qos(qos: Optional[Dict] = None) -> Optional[List]:
    # TODO
    pass

def get_ess_df(ess_filepath: str = "") -> Optional[pd.DataFrame]:
    if ess_filepath == "":
        logger.error(
            f"No filepath passes for ESS."
        )
        return None

    ess_exists = os.path.exists(ess_filepath)
    if ess_exists:
        ess_df = pd.read_csv(ess_filepath)
    else:
        ess_df = pd.DataFrame(columns=[
            "start_timestamp",
            "end_timestamp",
            "test_name",
            "pings_count",
            "ssh_check_count",
            "end_status",
            "attempt_number"
       ])

    return ess_df

def main(sys_args: list[str] = []) -> None:
    if len(sys_args) < 2:
        logger.error(
            f"Config filepath not specified."
        )
        return None

    CONFIG_PATH = sys_args[1]
    if not os.path.exists(CONFIG_PATH):
        logger.error(
            f"Config path {CONFIG_PATH} does NOT exist."
        )
        return None

    logger.debug(f"Reading {CONFIG_PATH}.")

    CONFIG = read_config(CONFIG_PATH)
    if CONFIG is None:
        logger.error(
            f"Couldn't read config of {CONFIG_PATH}."
        )
        return None

    for EXPERIMENT_INDEX, EXPERIMENT in enumerate(CONFIG):

        logger.debug(f"[{EXPERIMENT_INDEX + 1}/{len(CONFIG)}] Running {EXPERIMENT['experiment_name']}...")

        EXPERIMENT_DIRNAME = get_dirname_from_experiment(EXPERIMENT)
        if EXPERIMENT_DIRNAME is None:
            logger.error(
                f"Error getting experiment dirname for {EXPERIMENT['experiment_name']}"
                )
            continue

        if not os.path.exists(EXPERIMENT_DIRNAME):
            os.makedirs(EXPERIMENT_DIRNAME)

        is_pcg = get_if_pcg(EXPERIMENT)
        if is_pcg is None:
            logger.error(
                f"Error checking if {EXPERIMENT['experiment_name']} is PCG or not."
            )
            continue 

        if is_pcg:
            COMBINATIONS = generate_combinations_from_qos(EXPERIMENT['qos_settings'])

            ESS_FILEPATH = os.path.join(EXPERIMENT_DIRNAME, 'ess.csv')
            ess_df = get_ess_df(ESS_FILEPATH)

if __name__ == "__main__":
    if pytest.main(["-q", "./pytests", "--exitfirst"]) == 0:
        main(sys.argv)
    else:
        logger.error("Tests failed.")
        sys.exit(1)
