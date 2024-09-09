import subprocess
import time
import sys
import itertools
import re
import os
import logging
import json
import paramiko
import warnings

from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pprint import pprint
from rich.table import Table
from rich.console import Console
from rich.markdown import Markdown
from rich.live import Live
from rich.spinner import Spinner
from rich.logging import RichHandler
from io import StringIO

console = Console(record=True)

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd

DEBUG_MODE = False

logging.basicConfig(
    level=logging.DEBUG,
    filename="logs/autoperf_monitor_refactor.log",
    filemode="w",
    format='%(message)s'
)

logger = logging.getLogger(__name__)

console_handler = RichHandler(markup=True)
if DEBUG_MODE:
    console_handler.setLevel(logging.DEBUG)
else:
    console_handler.setLevel(logging.ERROR)

formatter = logging.Formatter(
    "%(message)s"
)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

REQUIRED_MACHINE_KEYS = [
    'name',
    'ip',
    'username',
    'ssh_key_path',
    'config_path'
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

PERCENTILES = [
    0, 1, 2, 3, 4, 5, 10,
    20, 30, 40, 60, 70, 80, 90,
    95, 96, 97, 98, 99, 100,
    25, 50, 75
]

def ping_machine(ip: str = "") -> Optional[bool]:
    """
    Ping a machine to check if it's up.

    Params:
        ip: str: IP address of machine to ping.
    Returns:
        bool: True if machine is up, False if machine is down.
    """
    if ip == "":
        logger.error(
            f"No IP passed for connection check."
        )
        return None

    # logger.debug(
    #     f"Pinging {ip}"
    # )

    response = os.system(f"ping -c 1 {ip} > /dev/null 2>&1")

    if response == 0:
       return True
    else:
        return False

def check_connection(machine, connection_type="ping"):
    # TODO: Validate parameters 
    # connection type can be only ping or ssh

    username = machine['username']
    name = machine['name']
    ip = machine['ip']
    
    if connection_type == "ping":
        logger.debug(f"Pinging {name} ({ip})")
        command = ["ping", "-c", "5", "-W", "10", ip]
    else:
        logger.debug(f"SSHing into {name} ({ip})")
        command = [
            "ssh",
            "-o", "ConnectTimeout=10",
            f"{username}@{ip}",
            "hostname"
        ]

    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            return False, result.stderr.strip()
        else:
            return True, None

    except subprocess.TimeoutExpired:
        return False, "Timed out"
    except Exception as e:
        return False, e

def get_difference_between_lists(list_one: List = [], list_two: List = []):
    """
    Get the difference between two lists.

    Params:
        list_one: List: First list.
        list_two: List: Second list.

    Returns:
        List: List containing elements that are in list_one but not in list_two
    """
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
    """
    Get the longer list between two lists.

    Params:
        list_one: List: First list.
        list_two: List: Second list.

    Returns:
        List: The longer list.
    """
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
    """
    Get the shorter list between two lists.

    Params:
        list_one: List: First list.
        list_two: List: Second list.

    Returns:
        List: The shorter list.
    """
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
    """
    Validate a dictionary using required keys.

    Params:
        given_keys: List: List of keys given.
        required_keys: List: List of required keys.

    Returns:
        bool: True if all required keys are present, False if not.
    """
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
        given_keys = "\n\t".join(given_keys)
        list_difference = "\n\t".join(list_difference)
        required_keys = "\n\t".join(required_keys)
        logger.error(
            f"Mismatch in keys for \n\t{given_keys}\nand: \n\t{list_difference}\nrequired: \n\t{required_keys}"
        )
        return False
    
    return True

def read_config(config_path: str = ""):
    """
    Read a JSON config file.

    Params:
        config_path: str: Path to the JSON config file.

    Returns:
        List: List of dictionaries containing machine configurations

    """

    logger.debug(f"Reading config file: {config_path}")
    if config_path == "":
        logger.error(
            f"No config path passed to read_config()"
        )
        return None

    with open(config_path, 'r') as f:
        try:
            config = json.load(f)
        except ValueError as e:
            logger.error(
                f"Error parsing JSON for config file: {config_path}: \n\t{e}"
            )
            return None

    if not isinstance(config, list):
        logger.error(
            f"Config file does not contain a list: {config_path}"
        )
        return None

    for machine_config in config:
        if not isinstance(machine_config, dict):
            logger.error(
                f"Config file contains non-dictionary element: {machine_config}"
            )
            return None

        keys = machine_config.keys()
        if keys is None:
            logger.error(
                f"Keys not found in machine config: {machine_config}"
            )
            return
        if not validate_dict_using_keys(
            keys, 
            REQUIRED_MACHINE_KEYS
        ):
            logger.error(
                f"Invalid keys in machine config: {machine_config}"
            )
            return None

        for key in keys:
            if key == "ip":
                ip = machine_config[key]
                if ip.split('.') == 4:
                    logger.error(
                        f"Invalid IP address: {ip}"
                    )
                    return None
                    
    return config

def get_valid_dirname(
    dir_name: str = ""
) -> Tuple[Optional[str], Optional[str]]:
    """
    Validate a directory name by removing any special characters and spaces.

    Params:
        dir_name: str: Directory name to validate.

    Returns:
        str: Valid directory name if valid, None if not.
        str: Error message if not.
    """
    if dir_name == "":
        return None, f"No dirname passed for validation."

    dir_name = re.sub(r'[<>:"/\\|?*]', '_', dir_name)
    dir_name = dir_name.strip()
    dir_name = re.sub(r'\s+', '_', dir_name)

    if not dir_name:
        return None, f"Dirname can't be empty after validation."

    if len(dir_name) > 255:
        return None, f"Dirname can't be more than 255 characters:\n\t{dir_name}"

    return dir_name, None

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

    Params:
        test_name: str: Test name to get qos settings from.

    Returns:
        Dict: QoS settings if successful, None if not.
    """
    if test_name == "":
        logger.error(
            "No test name passed to get_qos_dict_from_test_name()."
        )
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
        "use_reliable": None
    }

    if "." in test_name:
        test_name = test_name.split('.')[0]

    if "_" not in test_name:
        logger.error(
            "No _ found in test name: {test_name}"
        )
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

            qos_dict['use_reliable'] = use_reliable

        elif "mc" in section.lower() or 'uc' in section.lower():
            use_multicast = None

            if 'mc' in section.lower():
                use_multicast = True
            else:
                use_multicast = False

            qos_dict['use_multicast'] = use_reliable

        elif section.lower().endswith('b'):
            value = section.lower().replace("b", "")
            value = int(value)
            qos_dict["datalen_bytes"] = value

        else:
            logger.error(
                f"Couldn't recognise following section: {section}"
            )
            return None

    # Final check for any None values
    for key, value in qos_dict.items():
        if value is None:
            logger.error(
                f"Value for {key} is None."
            )
            return None

    return qos_dict

def get_latest_config_from_machine(
    machine_config: Dict = {}
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Get the latest config file used on a machine by checking the bash history.

    Params:
        machine_config: Dict: Dictionary containing machine configuration including name, ip, username, ssh_key_path.

    Returns:
        Dict: Dictionary containing the latest config file used if successful,
        error: str: Error message if not.
    """
    if machine_config == {}:
        return None, "No machine config passed."
        
    last_autoperf_command = "tail -n 50 ~/.bash_history | grep \"python autoperf.py\" | tail -n 1"
    last_autoperf_output = run_command_via_ssh(
        machine_config,
        last_autoperf_command
    )
    if last_autoperf_output is None:
        logger.error(
            f"Couldn't get last autoperf command from {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    last_autoperf_output = last_autoperf_output.split()
    if len(last_autoperf_output) == 0:
        logger.error(
            f"No autoperf commands found on {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    last_autoperf_output = last_autoperf_output[-1]
    config_file_used = last_autoperf_output.split("autoperf.py")
    if len(config_file_used) == 0:
        logger.error(
            f"No autoperf commands found on {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    config_file_used = config_file_used[-1].strip()

    console.print(f"Last config file used: {config_file_used}", style="bold green")
        
    logger.debug(
        f"Using config file: {config_file_used}"
    )

    config_filepath = os.path.join("~/AutoPerf", config_file_used)

    read_config_command = f"cat {config_filepath}"
    config_contents = run_command_via_ssh(
        machine_config,
        read_config_command
    )
    if config_contents is None:
        logger.error(
            f"Couldn't read config file from {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    config_dict = json.loads(config_contents)
    if config_dict is None:
        logger.error(
            f"Couldn't parse config file from {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    return config_dict

def generate_combinations_from_qos(qos: Optional[Dict] = None) -> Optional[List]:
    """
    Generate combinations from QoS settings.

    Params:
        qos: Dict: Dictionary containing QoS settings.

    Returns:
        List: List of dictionaries containing combinations of QoS settings.
    """
    if qos is None:
        logger.error(
            f"No QoS passed."
        )
        return None


    keys = qos.keys()
    if len(keys) == 0:
        logger.error(
            f"No options found for qos"
        )
        return None

    for key in keys:
        if key not in REQUIRED_QOS_KEYS:
            logger.error(
                f"Found an unexpected QoS setting: {key}"
            )
            return None

    values = qos.values()
    if len(values) == 0:
        logger.error(
            f"No values found for qos"
        )
        return None
    for value in values:
        if len(value) == 0:
            logger.error(
                f"One of the settings has no values."
            )
            return None

    combinations = list(itertools.product(*values))
    combination_dicts = [dict(zip(keys, combination)) for combination in combinations]

    return combination_dicts

def calculate_pcg_target_test_count(experiment_config: Dict = {}) -> Optional[int]:
    """
    Calculate the target test count for PCG experiments by generating combinations from QoS settings.

    Params:
        experiment_config: Dict: Dictionary containing experiment configuration.

    Returns:
        int: Target test count if successful, None if not.
    """

    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    qos = experiment_config['qos_settings']
    combinations = generate_combinations_from_qos(qos)
    if combinations is None:
        logger.error(
            f"Couldn't get combinations from qos."
        )
        return None

    target_test_count = len(combinations)
    return target_test_count
   
def calculate_target_test_count_for_experiment(
    experiment: Dict = {}
) -> Tuple[Optional[int], Optional[str]]:
    """
    Calculate the target test count for experiments by generating combinations from QoS settings.

    Params:
        config: Dict: Dictionary containing experiment configuration.

    Returns:
        Dict: Experiment configuration with target test count if successful, None if not.
        str: Error message if not.
    """
    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    is_pcg = experiment['combination_generation_type'] == "pcg"

    if not is_pcg:
        return experiment['rcg_target_test_count'], None
    else:
        target_test_count = calculate_pcg_target_test_count(experiment)
        return target_test_count, None

def get_dirname_from_experiment(
    experiment: Optional[Dict] = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get the name of the folder where the data is stored for the experiment.

    Params:
        experiment: Dict: Dictionary containing experiment configuration.

    Returns:
        str: Dirname if successful,
        str: Error message if not.
    """
    if experiment is None:
        return None, f"No experiment config passed."

    experiment_name = experiment['experiment_name']
    experiment_dirname = get_valid_dirname(experiment_name)
    if experiment_dirname is None:
        return None, f"Couldn't get valid dirname for {experiment_name}."

    experiment_dirname = os.path.join("output/data", experiment_dirname)

    return experiment_dirname, None

def calculate_completed_tests_for_experiments(config: Dict = {}, machine_config: Dict = {}) -> Optional[Dict]:
    """
    Calculate the completed tests for experiments by counting the number of files in the experiment directory.

    Params:
        config: Dict: Dictionary containing experiment configuration.
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: Experiment configuration with completed tests if successful, None
    """

    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    for experiment in config:
        experiment_dirname, error = get_dirname_from_experiment(experiment)
        if error:
            logger.warning(f"Couldn't get experiment dirname for {experiment['experiment_name']}.")
            continue

        experiment_dirname = os.path.join("~/AutoPerf", experiment_dirname)

        check_dir_exists_command = f"ls {experiment_dirname}"
        check_dir_exists_output = run_command_via_ssh(
            machine_config, 
            check_dir_exists_command
        )
        if check_dir_exists_output is None:
            logger.error(
                f"Couldn't check if {experiment_dirname} exists on {machine_config['name']} ({machine_config['ip']})."
            )
            continue

        if "No such file or directory" in check_dir_exists_output:
            logger.error(
                f"{experiment_dirname} doesn't exist on {machine_config['name']} ({machine_config['ip']})."
            )
            continue

        dir_contents = check_dir_exists_output.split()        
        pprint(dir_contents)
        dir_contents = [_ for _ in dir_contents if not _.endswith(".csv") and not _.startswith("summarised_data")]
        
        experiment['completed_tests'] = dir_contents

    return config

def check_for_zip_results(config: Dict = {}, machine_config: Dict = {}) -> Optional[Dict]:
    """
    Check if zip results exist for experiments in the /data directory.

    Params:
        config: Dict: Dictionary containing experiment configuration.
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: Experiment configuration with zip results if successful, None
    """
    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    get_zips_command = "ls ~/AutoPerf/output/data/*.zip"
    get_zips_output = run_command_via_ssh(
        machine_config,
        get_zips_command
    )
    if get_zips_output is None:
        logger.error(
            f"Couldn't get zip files from {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    zips = get_zips_output.split()
    zips = [os.path.basename(_) for _ in zips]

    for experiment in config:
        experiment_dirname = get_dirname_from_experiment(experiment)
        if experiment_dirname is None:
            logger.error(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}."
            )
            continue

        experiment_name = os.path.basename(experiment_dirname)
        experiment_zip_filename = f"{experiment_name}.zip"

        if experiment_zip_filename not in zips:
            experiment['zip_results_exist'] = False
        else:
            experiment['zip_results_exist'] = True

    return config

def get_ess_df_for_experiments(config: Dict = {}, machine_config: Dict = {}) -> Optional[Dict]:
    """
    Get the ESS DataFrame for experiments.

    Params:
        config: Dict: Dictionary containing experiment configuration.
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: Experiment configuration with ESS DataFrame if successful, None
    """
    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    for experiment in config:
        experiment_dirname = get_dirname_from_experiment(experiment)
        if experiment_dirname is None:
            logger.error(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}."
            )
            continue

        experiment_name = os.path.basename(experiment_dirname)
        ess_filepath = os.path.join("~/AutoPerf/output/ess", f"{experiment_name}.csv")

        ess_file_cat_command = f"cat {ess_filepath}"
        ess_file_cat_output = run_command_via_ssh(
            machine_config,
            ess_file_cat_command
        )
        if ess_file_cat_output is None:
            experiment['ess_df'] = None
            continue

        ess_file_contents = StringIO(ess_file_cat_output)
        try:
            ess_df = pd.read_csv(ess_file_contents)
        except pd.errors.EmptyDataError:
            experiment['ess_df'] = None
            continue
        except pd.errors.ParserError:
            experiment['ess_df'] = None
            continue
        
        experiment['ess_df'] = ess_df

    return config

def read_ap_config_from_machine(
    machine_config: Dict = {}
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Read the AutoPerf config from a machine.

    Params:
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: AutoPerf config if successful, None if not.
        str: Error message if not.
    """
    config_path = machine_config['config_path']
    if config_path == "":
        return None, "No config path passed."
        
    if not config_path.endswith(".json"):
        return None, "Config path doesn't end with .json."
        
    if "~" not in config_path:
        config_path = os.path.join("~/AutoPerf", config_path)

    console.print(f"Last config file used: {config_path}", style="bold green")

    read_config_command = f"cat {config_path}"
    config_contents, error = run_command_via_ssh(
        machine_config,
        read_config_command
    )
    if error or config_contents is None:
        return None, f"Couldn't read config file from {machine_config['name']} ({machine_config['ip']})."

    if config_contents.strip() == "":
        return None, f"Config file is empty on {machine_config['name']} ({machine_config['ip']})."

    try:
        config_dict = json.loads(config_contents)
    except json.JSONDecodeError as e:
        return None, f"Couldn't parse config file from {machine_config['name']} ({machine_config['ip']}): {e}"

    if config_dict is None:
        return None, f"Couldn't parse config file from {machine_config['name']} ({machine_config['ip']})."

    return config_dict, None

def get_folder_and_datasets_count_for_experiments(
    config: Dict = {}, 
    machine_config: Dict = {},
    status: Optional[Dict] = {}
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Get the folder count for experiments.

    Params:
        config: Dict: Dictionary containing experiment configuration.
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: Experiment configuration with folder count if successful, None
        str: Error
    """

    if config == {}: 
        return None, "No config passed."

    if machine_config == {}:
        return None, "No machine config passed."

    for experiment in config:
        experiment_dirname, error = get_dirname_from_experiment(experiment)
        if error:
            logger.warning(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}. {error}"
            )
            continue

        exp_name = os.path.basename(experiment_dirname)
        ess_filepath = os.path.join("~/AutoPerf/output/ess", f"{exp_name}.csv")

        data_path = os.path.join("~/AutoPerf", "output/data", exp_name)
        summ_data_path = os.path.join("~/AutoPerf", "output/summarised_data", exp_name)
        datasets_dir = os.path.join("~/AutoPerf", "output/datasets")
        ess_file_cat_command = f"echo 'CUT_HERE'; cat {ess_filepath}"

        count_folder_command = f"ls -l {data_path} | grep -c ^d; ls -l {summ_data_path} | grep -c ^d"
        list_datasets_command = f"ls -l {datasets_dir} | grep -o '.*{exp_name}.*'"
        get_datasets_command = f"ls -l {datasets_dir} | grep -o '.*{exp_name}.*'"

        full_command = f"{count_folder_command}; {list_datasets_command}; {get_datasets_command}; {ess_file_cat_command}"

        status.update(f"Getting data on {machine_config['name']} ({machine_config['ip']}) for {experiment['experiment_name']}.")
        command_output, error = run_command_via_ssh(
            machine_config,
            full_command
        )
        if error or command_output is None:
            logger.warning(
                f"Couldn't run commands \n{full_command}\n on {machine_config['name']} ({machine_config['ip']}). {error}"
            )
            continue

        folder_outputs = command_output.split("CUT_HERE")[0]
        folder_output = folder_outputs.split("\n")
        experiment['data'] = folder_output[0]
        experiment['summarised_data'] = folder_output[1]
        datasets = folder_output[2:-1]
        
        formatted_datasets = []
        for dataset in datasets:
            formatted_datasets.append(dataset.split(" ")[-1])

        experiment['datasets'] = formatted_datasets

        ess_output = command_output.split("CUT_HERE")[-1]
        ess_output = StringIO(ess_output)
        try:
            ess_df = pd.read_csv(ess_output)
            experiment['ess_df'] = ess_df
        except pd.errors.EmptyDataError:
            experiment['ess_df'] = None
            continue
        except pd.errors.ParserError:
            experiment['ess_df'] = None
            continue

    return config, None

def get_folder_count_for_experiment(
    experiment: Dict = {}, 
    machine_config: Dict = {}, 
    folder_path: str = ""
) -> Tuple[Optional[int], Optional[str]]:
    """
    Get the folder count for experiments.

    Params:
        config: Dict: Dictionary containing experiment configuration.
        machine_config: Dict: Dictionary containing machine configuration.
        folder_path: str: Path to the folder to count.

    Returns:
        int: Folder count if successful, None
        str: Error
    """

    if folder_path == "":
        return None, f"No folder path passed."

    if experiment == {}: 
        return None, f"No config passed."

    if machine_config == {}:
        return None, f"No machine config passed."

    experiment_dirname, error = get_valid_dirname(experiment['experiment_name'])
    if error or experiment_dirname is None:
        return None, f"Couldn't get experiment dirname for {experiment['experiment_name']}."

    exp_name = os.path.basename(experiment_dirname)

    dir_path = os.path.join("~/AutoPerf", folder_path, exp_name)

    count_folder_command = f"ls -l {dir_path} | grep -c ^d"

    ip = machine_config['ip']
    name = machine_config['name']
    username = machine_config['username']
    ssh_key_path = machine_config['ssh_key_path']

    logger.debug(f"Getting folder count for {experiment['experiment_name']} on {machine_config['name']} ({machine_config['ip']}).")

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip, username=username, key_filename=ssh_key_path)
        
        stdin, stdout, stderr = ssh.exec_command(count_folder_command)
        count_folder_output = stdout.read().decode().strip()
        count_folder_output = int(count_folder_output)
    
        return count_folder_output, None
    except SSHException as e:
        return None, f"Couldn't connect to {name} ({ip}): {e}"

def get_datasets_for_experiments(config: Dict = {}, machine_config: Dict = {}) -> Optional[Dict]:
    """
    Get the number of datasets for experiments.

    Params:
        config: Dict: Dictionary containing experiment configuration.
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: Experiment configuration with dataset count if successful, None
    """
    if config == {}:
        logger.error(
            f"No config passed."
        )
        return None

    if machine_config == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

    for experiment in config:
        if 'summarised_data' not in experiment.keys():
            logger.warning(
                f"summarised_data not found for {experiment['experiment_name']}."
            )
            experiment['summarised_data'] = '0'
            experiment['datasets'] = []
            continue

        # If there are no summarised_data then don't other checking for datasets
        if experiment['summarised_data'] == '0':
            experiment['datasets'] = []
            continue

        experiment_dirname = get_dirname_from_experiment(experiment)
        if experiment_dirname is None:
            logger.error(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}."
            )
            continue
        experiment_name = os.path.basename(experiment_dirname)

        datasets_dir = os.path.join("~/AutoPerf", "datasets")
        list_datasets_command = f"ls -l {datasets_dir} | grep -o '.*{experiment_name}.*'"
        list_datasets_output = run_command_via_ssh(
            machine_config,
            list_datasets_command
        )
        if list_datasets_output is None:
            logger.error(
                f"Couldn't count datasets in {datasets_dir} on {machine_config['name']} ({machine_config['ip']})."
            )
            experiment['datasets'] = []
            continue

        get_datasets_command = f"ls -l {datasets_dir} | grep -o '.*{experiment_name}.*'"
        get_datasets_command += " | awk '{print $9}'"
        get_datasets_output = run_command_via_ssh(
            machine_config,
            get_datasets_command
        )
        if get_datasets_output is None:
            logger.error(
                f"Couldn't get datasets in {datasets_dir} on {machine_config['name']} ({machine_config['ip']})."
            )
            experiment['datasets'] = []
            continue

        experiment['datasets'] = get_datasets_output.split()

    return config

def calculate_expected_time_for_experiments(config: Dict = {}) -> Optional[Dict]:
    """
    Calculate the expected total time for experiments by reading the config and multiplying target test count by duration.

    Params:
        config: Dict: Dictionary containing experiment configuration.

    Returns:
        Dict: Experiment configuration with expected total time if successful, None
    """
    if config == {}:
        logger.error(
            f"No config passed."
        )
        return None

    for experiment in config:
        if 'target_test_count' not in experiment.keys():
            logger.error(
                f"target_test_count not found for {experiment['experiment_name']}."
            )
            continue

        if 'qos_settings' not in experiment.keys():
            logger.error(
                f"duration_secs not found for {experiment['experiment_name']}."
            )
            continue

        target_test_count = experiment['target_test_count']
        duration_secs = max(experiment['qos_settings']['duration_secs'])

        expected_time_secs = target_test_count * duration_secs
        total_time_hours = expected_time_secs // 3600
        total_time_minutes = (expected_time_secs % 3600) // 60

        expected_time_str = f"{total_time_hours} hrs {total_time_minutes} mins"

        experiment['expected_time_sec'] = expected_time_secs
        experiment['expected_time_str'] = expected_time_str

    return config

def calculate_elapsed_time_for_experiments(config: Dict = {}) -> Optional[Dict]:
    """
    Calculate the elapsed time for experiments by reading the ESS DataFrame and subtracting the latest end time from the first start time.

    Params:
        config: Dict: Dictionary containing experiment configuration.

    Returns:
        Dict: Experiment configuration with elapsed time if successful, None
    """

    if config == {}:
        logger.error(
            f"No config passed."
        )
        return None

    for experiment in config:
        if 'ess_df' not in experiment.keys():
            logger.error(
                f"ess_df not found for {experiment['experiment_name']}."
            )
            continue

        ess_df = experiment['ess_df']
        if ess_df is None:
            logger.warning(
                f"ess_df is None for {experiment['experiment_name']}."
            )
            continue

        if ess_df.empty:
            logger.warning(
                f"ess_df is empty for {experiment['experiment_name']}."
            )
            continue

        ess_df['start_timestamp'] = pd.to_datetime(ess_df['start_timestamp'])
        ess_df['end_timestamp'] = pd.to_datetime(ess_df['end_timestamp'])

        start_time = ess_df['start_timestamp'].min()
        end_time = ess_df['end_timestamp'].max()

        if start_time is pd.NaT or end_time is pd.NaT:
            experiment['elapsed_time_str'] = "-"
            continue

        time_difference = end_time - start_time
        days, seconds = time_difference.days, time_difference.seconds
        hours = days * 24 + seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60

        elapsed_time_str = f"{hours} hrs {minutes} mins"
        experiment['elapsed_time_str'] = elapsed_time_str

    return config

def get_last_n_errors_for_experiments(ap_config: Dict = {}, n: int = 5) -> Optional[Dict]:

    for experiment in ap_config:
        if 'ess_df' not in experiment.keys():
            logger.error(
                f"ess_df not found for {experiment['experiment_name']}."
            )
            continue

        ess_df = experiment['ess_df']
        if ess_df is None:
            logger.warning(
                f"ess_df is None for {experiment['experiment_name']}."
            )
            continue

        if ess_df.empty:
            logger.warning(
                f"ess_df is empty for {experiment['experiment_name']}."
            )
            continue

        last_n_errors = ess_df['comments'].dropna().tail(n).tolist()
        if len(last_n_errors) == 0:
            experiment['last_n_errors'] = "-"
            continue
        elif len(last_n_errors) > 1:
            last_n_errors = "\n----------------\n".join(last_n_errors)
        else:
            last_n_errors = str(last_n_errors[0]) 
        experiment['last_n_errors'] = last_n_errors

    return ap_config

def get_ongoing_info_from_machine(
    machine_config: Dict = {}
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Get ongoing info from a machine.

    Params:
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: Updated machine_config if successful, None if not.
        str: Error message if not.
    """
    if machine_config == {}:
        return None, "No machine config passed."
        
    machine_name = machine_config['name']
    machine_ip = machine_config['ip']
    
    logger.debug(
        f"Monitoring ongoing tests on {machine_name} ({machine_ip})."
    )

    """
    - Get latest config from machine

    - Calculate target test count for experiments

    - Get number of datasets that exist with exp_name
    - Get number of files in /summarised_data/exp_name
    - Get number of folders in /data/exp_name

    - Get ESS for experiments
    - Get number of rows in ESS for experiments
    - Get number of successful tests
    - Get number of failed tests
    - Get status of last 100 tests

    - Get expected total time
    - Get elapsed time
    """

    with console.status(f"Getting data from {machine_name} ({machine_ip})...") as status:
        config_path = machine_config['config_path']

        if os.path.exists(config_path):
            ap_config = json.load(open(config_path, 'r'))
            
        else:
            if machine_config['config_path'] == "":
                ap_config, error = get_latest_config_from_machine(machine_config)
                if error:
                    return None, f"Couldn't get latest config from {machine_name}: {error}"
            else:
                ap_config, error = read_ap_config_from_machine(machine_config)
                if error:
                    return None, f"Couldn't read config from {machine_name}: {error}."

        status.update(f"Calculating target test count for experiments on {machine_name} ({machine_ip})...")
        ap_config, error = calculate_target_test_count_for_experiments(ap_config)
        if error:
            return None, f"Couldn't calculate target test count for experiments. {error}"

        exp_name, error = get_dirname_from_experiment(ap_config[0])
        if error or exp_name is None:
            return None, f"Couldn't get dirname for experiment."

        start_time = datetime.now()
        ap_config, error = get_folder_and_datasets_count_for_experiments(ap_config, machine_config, status)
        if error:
            return None, f"Couldn't get folder and datasets count for experiments: {error}"
        end_time = datetime.now()
        time_taken = (end_time - start_time).total_seconds()
        print(f"Time taken to get folder and dataset counts: {time_taken} seconds")

        ap_config = calculate_expected_time_for_experiments(ap_config)
        if ap_config is None:
            return None, f"Couldn't calculate expected total time for experiments."
        
        status.update(
            f"Calculating elapsed time for experiments on {machine_name} ({machine_ip})..."
        )
        ap_config = calculate_elapsed_time_for_experiments(ap_config)
        if ap_config is None:
            return None, f"Couldn't calculate elapsed time for experiments."

        status.update(f"Getting last n errors for experiments on {machine_name} ({machine_ip})...")
        ap_config = get_last_n_errors_for_experiments(ap_config, 3)
        if ap_config is None:
            return None, f"Couldn't get last 5 errors for experiments."

    return ap_config, None

def get_last_n_statuses_as_string_from_ess_df(
    ess_df: pd.DataFrame = pd.DataFrame(), 
    n: int = 0, 
    line_break_point: int = 10
) -> Tuple[Optional[str], Optional[Dict]]:
    """
    Get the last n statuses as a string of red or green circles from the ESS DataFrame.

    Params:
        ess_df: pd.DataFrame: DataFrame containing ESS data.
        n: int: Number of statuses to get.
        line_break_point: int: Number of statuses to display per line.

    Returns:
        str: String containing the last n statuses if successful, None if not.
    """
    # TODO: Write unit tests

    if ess_df is None:
        logger.warning(
            f"No ESS DataFrame passed."
        )
        return "", {}

    if ess_df.empty:
        logger.error(
            f"ESS DataFrame is empty."
        )
        return "", {}

    if n == 0:
        logger.error(
            f"No n passed."
        )
        return "", {}

    if n < 0:
        logger.error(
            f"Invalid n passed."
        )
        return "", {}

    if line_break_point < 0:
        logger.error(
            f"Invalid line_break_point passed."
        )
        return "", {}

    last_n_statuses = ess_df['end_status'].tail(n).tolist()
    all_emojis = ["🟠", "🟣", "🟡", "🔵", "🟤", "⚫", "⚪", "🟦", "🟧", "🟨", "🟩", "🟪", "🟫", "🟥", "🟦", "🟪", "🟧", "🟨", "🟩", "🟪", "🟫"]

    unique_statuses = ess_df['end_status'].unique().tolist()

    status_emoji_dict = {}
    for i, status in enumerate(unique_statuses):
        if "success" in status.lower():
            status_emoji_dict[status] = "🟢"
        elif status.lower().strip() == "fail":
            status_emoji_dict[status] = "🔴"
        else:
            status_emoji_dict[status] = all_emojis[i]

    last_n_statuses_output = ""
    for status in last_n_statuses:
        last_n_statuses_output += status_emoji_dict[status]

    # Add a line break after every line_break_point
    last_n_statuses_output = "\n".join(
        [
            last_n_statuses_output[i:i+line_break_point] for i in range(
                0, 
                len(last_n_statuses_output), 
                line_break_point
            )
        ]
    )

    # Sort the dict by status
    status_emoji_dict = {k: v for k, v in sorted(
        status_emoji_dict.items(), 
        key=lambda item: item[0]
    )}

    return last_n_statuses_output, status_emoji_dict

def get_ip_output_from_ess_df(ess_df, line_break_point: int = 10):
    if ess_df is None:
        return "", {}

    if 'ip' not in ess_df.columns:
        return "", {}

    ip_df = ess_df['ip'].dropna()

    unique_ips = ip_df.unique()
    all_emojis = ["🟠", "🟣", "🟡", "🔵", "🟤", "⚫", "⚪", "🟦", "🟧", "🟨", "🟩", "🟪", "🟫", "🟥", "🟦", "🟪", "🟧", "🟨", "🟩", "🟪", "🟫"]

    ip_emoji_dict = {}
    for i, ip in enumerate(unique_ips):
        ip = "xxx." + ip.split(".")[-1]
        if ip == "🟢":
            ip_emoji_dict[ip] = "🟢"

        ip_emoji_dict[ip] = all_emojis[i]

    # Go through each row, if the comment has success add green
    # if comment has fail add red
    # if comment has fail and IP then swap the IP with the emoji

    ip_output = ""
    for index, row in ess_df.iterrows():
        end_status = row['end_status']
        ip = str(row['ip'])
        ip = "xxx." + ip.split(".")[-1]

        if "success" in end_status.lower():
            ip_output += "🟢"
        elif "fail" in end_status.lower():
            if ip in ip_emoji_dict.keys():
                ip_output += ip_emoji_dict[ip]
            else:
                ip_output += "🔴"
        else:
            ip_output += "🔴"
        
    ip_output = "\n".join(
        [ip_output[i:i+line_break_point] for i in range(
            0, 
            len(ip_output), 
            line_break_point
        )]
    )

    # Sort the dict by IP number by removing the XXX. and converting to int
    ip_emoji_dict = {k: v for k, v in sorted(
        ip_emoji_dict.items(), 
        key=lambda item: int(
            item[0].replace("xxx.", "")
        )
    )}

    return ip_output, ip_emoji_dict

def get_last_timestamp_from_ess_df(
    ess_df: pd.DataFrame = pd.DataFrame()
) -> Tuple[Optional[str], Optional[str]]:
    """
    Get the last timestamp from the ESS DataFrame.

    Params:
        ess_df: pd.DataFrame: DataFrame containing ESS data.

    Returns:
        str: Last timestamp if successful, None if not.
    """
    if ess_df is None:
        return None, f"No ESS DataFrame passed."

    if ess_df.empty:
        return None, f"ESS DataFrame is empty."

    last_timestamp = ess_df['end_timestamp'].max()

    if last_timestamp is pd.NaT:
        return None, f"Last timestamp is NaT."

    last_timestamp = last_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return last_timestamp, None

def get_ip_fail_percent(ip, ess_df):
    if ess_df is None:
        return 0

    ip_df = ess_df['ip'].dropna()
    if ip_df.empty:
        return 0
    if len(ip_df.index) == 0:
        return 0

    ip_df = ip_df.dropna()
    ip_df = ip_df.apply(lambda x: "xxx." + x.split(".")[-1])
    total_ip_count = len(ip_df.index)

    ip_df = ip_df[ip_df == ip]
    current_ip_count = len(ip_df.index)

    ip_fail_percent = (current_ip_count / total_ip_count) * 100
    ip_fail_percent = round(ip_fail_percent, 1)

    return ip_fail_percent

def get_status_percentage_from_ess_df(ess_df, status):
    if ess_df is None:
        return 0

    status_df = ess_df['end_status'].dropna()
    if status_df.empty:
        return 0
    if len(status_df.index) == 0:
        return 0

    status_df = status_df.dropna()
    total_status_count = len(status_df.index)

    status_df = status_df[status_df == status]
    current_status_count = len(status_df.index)

    status_fail_percent = (current_status_count / total_status_count) * 100
    status_fail_percent = round(status_fail_percent, 1)

    return status_fail_percent

def create_empty_table() -> Table:
    table = Table()
    table.add_column("Experiment Name")
    table.add_column(
        "Elapsed\n-----\nExpected\nTime"
    )
    table.add_column(
        "Last\nTimestamp"
    )
    table.add_column(
        "Last\n100\nStatuses"
    )
    table.add_column(
        "Failed\nIPs"
    )
    table.add_column(
        "Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount"
    )

    table.add_row(
        "",
        "",
        Spinner("dots", text="Loading...", style="green"),
    )

    return table

def get_ap_config_from_machine(machine_config) -> Tuple[Optional[Dict], Optional[str]]:
    logger.debug(f"Getting AP config from {machine_config['name']} ({machine_config['ip']}).")
    if machine_config == {}:
        return None, "No machine config passed."

    # TODO: Validate parameters

    config_path = machine_config['config_path']
    if not os.path.exists(config_path):
        return None, "Config path doesn't exist."

    try:
        ap_config = json.load(open(config_path, 'r'))
    except json.JSONDecodeError as e:
        return None, f"Couldn't parse config file: {e}"

    return ap_config, None

def create_table(table_data: Dict = {}) -> Table:
    table = Table(show_lines=True)
    table.add_column("Experiment Name")
    table.add_column(
        "Elapsed\n-----\nExpected\nTime"
    )
    table.add_column(
        "Last\nTimestamp"
    )
    table.add_column(
        "Last\n100\nStatuses"
    )
    table.add_column(
        "Failed\nIPs"
    )
    data_str = "[red]Data[/red]"
    summ_data_str = "[blue]Summarised\nData[/blue]"
    datasets_str = "[green]Datasets[/green]"
    target_test_str = "[yellow]Target\nTest\nCount[/yellow]"
    table.add_column(
        f"{data_str}\n-----\n{summ_data_str}\n-----\n{datasets_str}\n-----\n{target_test_str}"
    )

    for key, value in table_data.items():
        table.add_row(
            key,
            *value.values()
        )

    return table

def run_ssh_command_with_paramiko(
    ip: str = "", 
    username: str = "", 
    ssh_key_path: str = "", 
    command: str = ""
) -> Tuple[
        Optional[str], Optional[str], Optional[str]
    ]:
    logger.debug(f"Running command: {command} on {ip}.")
    if ip == "":
        return None, None, "No IP passed."

    if username == "":
        return None, None, "No username passed."

    if ssh_key_path == "":
        return None, None, "No SSH key passed."

    if command == "":
        return None, None, "No command passed."

    if not os.path.exists(ssh_key_path):
        return None, None, f"SSH key doesn't exist: {ssh_key_path}"

    connection = paramiko.SSHClient()
    connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connection.connect(
        ip,
        username=username,
        key_filename=ssh_key_path
    )

    try:
        stdin, stdout, stderr = connection.exec_command(command)
        output = stdout.read().decode()
        error = stderr.read().decode()
    except paramiko.SSHException as e:
        return None, None, f"Couldn't run command on {ip}: {e}"

    return output, error, None

def get_ess_df(
    machine_config: Dict = {}, 
    campaign_config: Dict = {}
) -> Tuple[Optional[Dict], Optional[str]]:
    logger.debug(f"Getting ESS from {machine_config['name']} ({machine_config['ip']}).")
    if machine_config == {}:
        return None, "No machine config passed."

    if campaign_config == {}:
        return None, "No campaign config passed."

    ip = machine_config['ip']
    username = machine_config['username']
    ssh_key = machine_config['ssh_key_path']
    machine_name = machine_config['name']

    exp_name = campaign_config['experiment_name']
    exp_dirname, error = get_valid_dirname(exp_name)
    if error:
        return None, f"Couldn't get experiment dirname for {exp_name}: {error}"
    exp_ess_filename = f"{exp_dirname}.csv"

    full_exp_dirname = os.path.join("/home/acwh025/AutoPerf/output/ess/", exp_ess_filename)

    connection = paramiko.SSHClient()
    connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    connection.connect(
        ip,
        username=username,
        key_filename=ssh_key
    )
    sftp = connection.open_sftp()
    logger.debug(
        f"Downloading {exp_ess_filename} from {machine_name} ({ip})."
    )
    os.makedirs("./output/monitor/ess", exist_ok=True)

    try:
        sftp.stat(full_exp_dirname)
    except IOError as e:
        return None, f"{full_exp_dirname} doesn't exist on {machine_name} ({ip}): {e}"

    sftp.get(full_exp_dirname, f"./output/monitor/ess/{exp_ess_filename}")
    sftp.close()
    connection.close()

    ess_df = pd.read_csv(f"./output/monitor/ess/{exp_ess_filename}")
    return ess_df, None

def get_elapsed_time_from_ess(
    ess_df: pd.DataFrame = pd.DataFrame()
) -> Tuple[Optional[str], Optional[str]]:
    logger.debug(f"Getting elapsed time from ESS.")
    if ess_df is None:
        return None, "No ESS DataFrame passed."

    if ess_df.empty:
        return None, "ESS DataFrame is empty."

    ess_df['start_timestamp'] = pd.to_datetime(ess_df['start_timestamp'])
    ess_df['end_timestamp'] = pd.to_datetime(ess_df['end_timestamp'])

    start_time = ess_df['start_timestamp'].min()
    end_time = ess_df['end_timestamp'].max()

    if start_time is pd.NaT or end_time is pd.NaT:
        return None, "Start or end time is NaT."

    time_difference = end_time - start_time
    days, seconds = time_difference.days, time_difference.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    elapsed_time_str = f"{hours} hrs\n{minutes} mins"

    return elapsed_time_str, None

def get_expected_time_from_config(
    campaign_config: Dict = {}
) -> Tuple[Optional[str], Optional[str]]:
    if campaign_config == {}:
        return None, "No campaign config passed."

    target_test_count, error = calculate_target_test_count_for_experiment(campaign_config)
    if error:
        return None, f"Couldn't calculate target test count: {error}"

    duration_secs = max(campaign_config['qos_settings']['duration_secs'])
    expected_time_secs = target_test_count * duration_secs

    total_time_hours = expected_time_secs // 3600
    total_time_minutes = (expected_time_secs % 3600) // 60
    total_time_str = f"{total_time_hours} hrs\n{total_time_minutes} mins"

    return total_time_str, None

def main(sys_args: list[str] = []) -> Optional[str]:
    if len(sys_args) < 2:
        return f"Config filepath not specified."

    CONFIG_PATH = sys_args[1]
    if not os.path.exists(CONFIG_PATH):
        return f"Config path {CONFIG_PATH} does NOT exist."

    CONFIG = read_config(CONFIG_PATH)
    if CONFIG is None:
        return f"Couldn't read config of {CONFIG_PATH}."

    table = create_empty_table()
    loading_spinner = Spinner("dots", text="Loading...", style="green")

    for MACHINE_CONFIG in CONFIG:
        logger.debug(f"Monitoring {MACHINE_CONFIG['name']} ({MACHINE_CONFIG['ip']}).")
        table_data = {}

        with Live(table, refresh_per_second=8) as live:
            ap_config, error = get_ap_config_from_machine(MACHINE_CONFIG)
            if error or ap_config is None:
                logger.error(
                    f"Couldn't get AutoPerf config from {MACHINE_CONFIG['name']} ({MACHINE_CONFIG['ip']}). {error}"
                )
                continue

            for index, campaign in enumerate(ap_config):
                index_str = f"[{index + 1}/{len(ap_config)}]"
                logger.debug(f"{index_str} Monitoring {campaign['experiment_name']}.")

                campaign_data = {
                    "Elapsed\n-----\nExpected\nTime": loading_spinner,
                    "Last\nTimestamp": loading_spinner,
                    "Last\n100\nStatuses": loading_spinner,
                    "Failed\nIPs": loading_spinner,
                    "Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount": loading_spinner
                }

                experiment_name = campaign['experiment_name']
                experiment_name = experiment_name.replace(" ", "\n")
                table_data[experiment_name] = campaign_data
                updated_table = create_table(table_data)
                live.update(updated_table)

                ess_df, error = get_ess_df(MACHINE_CONFIG, campaign)
                if error or ess_df is None:
                    logger.warning(f"Error getting ESS: {error}")
                    error_keys = [
                        "Elapsed\n-----\nExpected\nTime",
                        "Last\nTimestamp",
                        "Last\n100\nStatuses",
                        "Failed\nIPs",
                        "Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount",
                    ]
                    for key in error_keys:
                        campaign_data[key] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                elapsed_time, error = get_elapsed_time_from_ess(ess_df)
                if error or elapsed_time is None:
                    logger.warning(f"Error getting elapsed time: {error}")
                    campaign_data['Elapsed\n-----\nExpected\nTime'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                expected_time, error = get_expected_time_from_config(campaign)
                if error or expected_time is None:
                    logger.warning(f"Error getting expected time: {error}")
                    campaign_data['Elapsed\n-----\nExpected\nTime'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue 

                time_str = f"{elapsed_time}\n-----\n{expected_time}"
                table_data[experiment_name]['Elapsed\n-----\nExpected\nTime'] = time_str
                updated_table = create_table(table_data)
                live.update(updated_table)

                last_timestamp, error = get_last_timestamp_from_ess_df(ess_df)
                if error or last_timestamp is None:
                    logger.warning(f"Error getting last timestamp: {error}")
                    campaign_data['Last\nTimestamp'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                table_data[experiment_name]['Last\nTimestamp'] = last_timestamp.replace(" ", "\n")
                updated_table = create_table(table_data)
                live.update(updated_table)

                last_n_statuses, status_emoji_dict = get_last_n_statuses_as_string_from_ess_df(ess_df, 100)
                if last_n_statuses is None or status_emoji_dict is None:
                    logger.warning(f"Error getting last n statuses: {error}")
                    campaign_data['Last\n100\nStatuses'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                last_n_statuses_legend = ""
                for status, emoji in status_emoji_dict.items():
                    last_n_statuses_legend += f"{emoji}: {status}\n"
                last_n_statuses_legend = last_n_statuses_legend.strip()
                last_n_statuses = f"{last_n_statuses}\n\n{last_n_statuses_legend}"
                table_data[experiment_name]['Last\n100\nStatuses'] = last_n_statuses
                updated_table = create_table(table_data)
                live.update(updated_table)

                ip_output, ip_emoji_dict = get_ip_output_from_ess_df(ess_df)
                if ip_output is None:
                    logger.warning(f"Error getting IP output: {error}")
                    campaign_data['Failed\nIPs'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue
                elif ip_output == "":
                    campaign_data['Failed\nIPs'] = "-"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                ip_output_legend = ""
                for ip, emoji in ip_emoji_dict.items():
                    ip_output_legend += f"{emoji}: {ip}\n"
                ip_output_legend = ip_output_legend.strip()
                ip_output = f"{ip_output}\n\n{ip_output_legend}"
                table_data[experiment_name]['Failed\nIPs'] = ip_output
                updated_table = create_table(table_data)
                live.update(updated_table)

                data_count, error = get_folder_count_for_experiment(campaign, MACHINE_CONFIG, "output/data")
                if error or data_count is None:
                    logger.warning(f"Error getting data count: {error}")
                    campaign_data['Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                summarised_data_count, error = get_folder_count_for_experiment(campaign, MACHINE_CONFIG, "output/summarised_data")
                if error or summarised_data_count is None:
                    logger.warning(f"Error getting summarised data count: {error}")
                    campaign_data['Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                datasets_count, error = get_folder_count_for_experiment(campaign, MACHINE_CONFIG, "output/datasets")
                if error or datasets_count is None:
                    logger.warning(f"Error getting datasets count: {error}")
                    campaign_data['Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                target_test_count, error = calculate_target_test_count_for_experiment(campaign)
                if error or target_test_count is None:
                    logger.warning(f"Error getting target test count: {error}")
                    campaign_data['Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount'] = "[red]Error"
                    updated_table = create_table(table_data)
                    live.update(updated_table)
                    continue

                data_str = f"[red]{data_count}[/red]"
                summ_data_str = f"[blue]{summarised_data_count}[/blue]"
                datasets_str = f"[green]{datasets_count}[/green]"
                target_test_str = f"[yellow]{target_test_count}[/yellow]"
                data_str = f"{data_str}\n-----\n{summ_data_str}\n-----\n{datasets_str}\n-----\n{target_test_str}"
                table_data[experiment_name]['Data\n-----\nSummarised\nData\n-----\nDatasets\n-----\nTarget\nTest\nCount'] = data_str
                updated_table = create_table(table_data)
                live.update(updated_table)

            os.system('clear')
            console.print(updated_table)
            timestamp = datetime.now().strftime("%Y-%m-%d %H-%M-%S")
            config_name = os.path.basename(MACHINE_CONFIG['config_path'])
            console.save_html(f"./output/monitor/{timestamp} {MACHINE_CONFIG['name']} {config_name}.html")
    
    return None

if __name__ == "__main__":
    start_time = datetime.now()
    error = main(sys.argv)
    if error:
        logger.error(error)
    end_time = datetime.now()
    time_taken = (end_time - start_time).total_seconds()
    time_taken = round(time_taken, 2)
    console.print(f"Ran in {time_taken} seconds.")
