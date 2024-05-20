import pandas as pd
import sys
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

def validate_dict_using_keys(
    given_keys: List = [], 
    required_keys: List = []
) -> Optional[bool]:
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

if __name__ == "__main__":
    if pytest.main(["-q", "./pytests", "--exitfirst"]) == 0:
        main(sys.argv)
    else:
        logger.error("Tests failed.")
        sys.exit(1)
