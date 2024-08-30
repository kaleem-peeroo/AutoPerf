import subprocess
import sys
import itertools
import re
import os
import logging
import json
import warnings

from datetime import datetime
from icecream import ic
from typing import Dict, List, Optional, Tuple
from pprint import pprint
from multiprocessing import Process, Manager
from rich.progress import track
from rich.table import Table
from rich.console import Console
from rich.markdown import Markdown
from io import StringIO

console = Console()

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd

DEBUG_MODE = False

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, 
    filename="logs/autoperf_monitor.log", 
    filemode="w",
    format='%(asctime)s \t%(levelname)s \t%(message)s'
)
logger = logging.getLogger(__name__)

console_handler = logging.StreamHandler()
if DEBUG_MODE:
    console_handler.setLevel(logging.DEBUG)
else:
    console_handler.setLevel(logging.ERROR)
formatter = logging.Formatter(
    '%(asctime)s \t%(levelname)s \t%(message)s'
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

def check_ssh_connection(machine_config: Dict = {}) -> Optional[bool]:
    """
    Check if SSH connection can be established to a machine

    Params:
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        bool: True if SSH connection can be established, False if it can't.
    """
    if machine_config == {}:
        logger.error(
            f"No machine config passed to check_ssh_connection()."
        )
        return None
    
    ssh_key_path = machine_config['ssh_key_path']
    username = machine_config['username']
    ip = machine_config['ip']

    # logger.debug(
    #     f"Checking SSH connection to {username}@{ip}"
    # )

    response = os.system(f"ssh -i {ssh_key_path} {username}@{ip} 'echo \"SSH connection successful.\"' > /dev/null 2>&1")

    if response == 0:
        return True
    else:
        return False

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
        logger.error(
            f"Mismatch in keys for \n\t{given_keys}: \n\t\t{list_difference}"
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

def get_valid_dirname(dir_name: str = "") -> Optional[str]:
    """
    Validate a directory name by removing any special characters and spaces.

    Params:
        dir_name: str: Directory name to validate.

    Returns:
        str: Valid directory name if valid, None if not.
    """
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

def run_command_via_ssh(machine_config: Dict = {}, command: str = "") -> Optional[str]:
    """
    Run a command on a machine via SSH.

    Params:
        machine_config: Dict: Dictionary containing machine configuration including name, ip, username, ssh_key_path.
        command: str: Command to run on the machine.

    Returns:
        str: Output of the command if successful, None if not.
    """
    if machine_config == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

    if command == "":
        logger.error(
            f"No command passed."
        )
        return None

    machine_name = machine_config['name']
    machine_ip = machine_config['ip']
    username = machine_config['username']
    ssh_key = machine_config['ssh_key_path']

    logger.debug(
        f"Running {command} on {machine_name} ({machine_ip})."
    )

    if not ping_machine(machine_ip):
        logger.error(
            f"Couldn't ping {machine_name} ({machine_ip})."
        )
        return None

    if not check_ssh_connection(machine_config):
        logger.error(
            f"Couldn't SSH into {machine_name} ({machine_ip})."
        )
        return None

    ssh_command = f"ssh -i {ssh_key} {username}@{machine_ip} '{command}'"
    command_process = subprocess.Popen(
        ssh_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = command_process.communicate(timeout=30)
    stdout = stdout.decode('utf-8').strip()
    stderr = stderr.decode('utf-8').strip()

    if command_process.returncode != 0:
        if "No such file or directory" not in stderr:
            if stderr.strip() == "":
                return stdout
            else:
                logger.error(
                    f"\nError running {command} over SSH on {machine_name}:\n\t{stderr}\n"
                )
                return None

        if "No such file or directory" in stderr:
            if "summarised_data" in command:
               return stdout
            elif "data" in command:
                return stdout
         
    return stdout

def get_latest_config_from_machine(machine_config: Dict = {}) -> Optional[Dict]:
    """
    Get the latest config file used on a machine by checking the bash history.

    Params:
        machine_config: Dict: Dictionary containing machine configuration including name, ip, username, ssh_key_path.

    Returns:
        Dict: Dictionary containing the latest config file used if successful,
    """
    if machine_config == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

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
   
def calculate_target_test_count_for_experiments(config: Dict = {}) -> Optional[Dict]:
    """
    Calculate the target test count for experiments by generating combinations from QoS settings.

    Params:
        config: Dict: Dictionary containing experiment configuration.

    Returns:
        Dict: Experiment configuration with target test count if successful, None if not.
    """
    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    for experiment in config:
        is_pcg = experiment['combination_generation_type'] == "pcg"

        if not is_pcg:
            experiment['target_test_count'] = experiment['rcg_target_test_count']
        else:
            experiment['target_test_count'] = calculate_pcg_target_test_count(experiment)

    return config

def get_dirname_from_experiment(experiment: Optional[Dict] = None) -> Optional[str]:
    """
    Get the name of the folder where the data is stored for the experiment.

    Params:
        experiment: Dict: Dictionary containing experiment configuration.

    Returns:
        str: Dirname if successful,
    """
    if experiment is None:
        logger.error(
            f"No experiment config passed."
        )
        return None

    experiment_name = experiment['experiment_name']
    experiment_dirname = get_valid_dirname(experiment_name)
    if experiment_dirname is None:
        logger.error(
            f"Couldn't get valid dirname for {experiment_name}."
        )
        return None
    experiment_dirname = os.path.join("output/data", experiment_dirname)

    return experiment_dirname

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
        experiment_dirname = get_dirname_from_experiment(experiment)
        if experiment_dirname is None:
            logger.error(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}."
            )
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

def read_ap_config_from_machine(machine_config: Dict = {}) -> Optional[Dict]:
    """
    Read the AutoPerf config from a machine.

    Params:
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: AutoPerf config if successful, None if not.
    """
    config_path = machine_config['config_path']
    if config_path == "":
        logger.error(
            f"No config path passed to read_ap_config_from_machine()"
        )
        return None

    if not config_path.endswith(".json"):
        logger.error(
            f"Invalid config path passed: {config_path}"
        )
        return None

    if "~" not in config_path:
        config_path = os.path.join("~/AutoPerf", config_path)

    console.print(f"Last config file used: {config_path}", style="bold green")

    read_config_command = f"cat {config_path}"
    config_contents = run_command_via_ssh(
        machine_config,
        read_config_command
    )
    if config_contents is None:
        logger.error(
            f"Couldn't read config file from {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    if config_contents.strip() == "":
        logger.error(
            f"Config file is empty on {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    try:
        config_dict = json.loads(config_contents)
    except json.JSONDecodeError as e:
        logger.error(
            f"Couldn't parse config file from {machine_config['name']} ({machine_config['ip']}): {e}"
        )
        return None

    if config_dict is None:
        logger.error(
            f"Couldn't parse config file from {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    return config_dict

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
        experiment_dirname = get_dirname_from_experiment(experiment)
        if experiment_dirname is None:
            logger.warning(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}."
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
        command_output = run_command_via_ssh(
            machine_config,
            full_command
        )
        if command_output is None:
            logger.warning(
                f"Couldn't run commands \n{full_command}\n on {machine_config['name']} ({machine_config['ip']})."
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

def get_folder_count_for_experiments(config: Dict = {}, machine_config: Dict = {}, folder_path: str = "") -> Optional[Dict]:
    """
    Get the folder count for experiments.

    Params:
        config: Dict: Dictionary containing experiment configuration.
        machine_config: Dict: Dictionary containing machine configuration.
        folder_path: str: Path to the folder to count.

    Returns:
        Dict: Experiment configuration with folder count if successful, None
    """

    if folder_path == "":
        logger.error(
            f"No folder path passed."
        )
        return None

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
        experiment_dirname = get_dirname_from_experiment(experiment)
        if experiment_dirname is None:
            logger.error(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}."
            )
            continue
        exp_name = os.path.basename(experiment_dirname)

        dir_path = os.path.join("~/AutoPerf", folder_path, exp_name)

        count_folder_command = f"ls -l {dir_path} | grep -c ^d"
        count_folder_output = run_command_via_ssh(
            machine_config,
            count_folder_command
        )
        if count_folder_output is None:
            logger.error(
                f"Couldn't count folders in {dir_path} on {machine_config['name']} ({machine_config['ip']})."
            )
            continue

        experiment[os.path.basename(folder_path)] = count_folder_output

    return config

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

def get_ongoing_info_from_machine(machine_config: Dict = {}) -> Optional[Dict]:
    """
    Get ongoing info from a machine.

    Params:
        machine_config: Dict: Dictionary containing machine configuration.

    Returns:
        Dict: Updated machine_config if successful, None if not.
    """
    if machine_config == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

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
        if machine_config['config_path'] == "":
            ap_config = get_latest_config_from_machine(machine_config)
            if ap_config is None:
                logger.error(
                    f"Couldn't get latest config from {machine_name} ({machine_ip})."
                )
                return
        else:
            ap_config = read_ap_config_from_machine(machine_config)
            if ap_config is None:
                logger.error(
                    f"Couldn't read config from {machine_name} ({machine_ip})."
                )
                return

        status.update(f"Calculating target test count for experiments on {machine_name} ({machine_ip})...")
        ap_config = calculate_target_test_count_for_experiments(ap_config)
        if ap_config is None:
            logger.error(
                f"Couldn't calculate target test count for experiments."
            )
            return

        exp_name = get_dirname_from_experiment(ap_config[0])
        if exp_name is None:
            logger.error(
                f"Couldn't get dirname for experiment."
            )
            return

        ap_config, error = get_folder_and_datasets_count_for_experiments(ap_config, machine_config, status)
        if error:
            logger.error(
                f"Couldn't get folder and datasets count for experiments: {error}"
            )
            return

        status.update(f"Calculating expected total time for experiments on {machine_name} ({machine_ip})...")
        ap_config = calculate_expected_time_for_experiments(ap_config)
        if ap_config is None:
            logger.error(
                f"Couldn't calculate expected total time for experiments."
            )
            return
        
        status.update(f"Calculating elapsed time for experiments on {machine_name} ({machine_ip})...")
        ap_config = calculate_elapsed_time_for_experiments(ap_config)
        if ap_config is None:
            logger.error(
                f"Couldn't calculate elapsed time for experiments."
            )
            return

        status.update(f"Getting last n errors for experiments on {machine_name} ({machine_ip})...")
        ap_config = get_last_n_errors_for_experiments(ap_config, 3)
        if ap_config is None:
            logger.error(
                f"Couldn't get last 5 errors for experiments."
            )
            return

    return ap_config

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

def get_ip_output_from_ess_df(ess_df, line_break_point: int = 5):
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

def get_last_timestamp_from_ess_df(ess_df: pd.DataFrame = pd.DataFrame()) -> Optional[str]:
    """
    Get the last timestamp from the ESS DataFrame.

    Params:
        ess_df: pd.DataFrame: DataFrame containing ESS data.

    Returns:
        str: Last timestamp if successful, None if not.
    """
    if ess_df is None:
        logger.warning(
            f"No ESS DataFrame passed."
        )
        return None

    if ess_df.empty:
        logger.error(
            f"ESS DataFrame is empty."
        )
        return None

    last_timestamp = ess_df['end_timestamp'].max()

    if last_timestamp is pd.NaT:
        return "-"

    last_timestamp = last_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return last_timestamp

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

def display_as_table(ongoing_info: Dict = {}) -> Optional[None]:
    """
    Display ongoing info as a table with the following columns:
    - Experiment Name
    - Target Test Count
    - /data
    - /summarised_data
    - Datasets
    - ESS Status
    - Last 100 Statuses

    Params:
        ongoing_info: Dict: Dictionary containing ongoing info.

    Returns:
        None
    """
    # TODO: Validate parameters

    table = Table(
        title="Experiments Overview", show_lines=True
    )
    table.add_column(
        "Experiment Name", 
        style="bold"
    )
    table.add_column(
        "Elapsed\n-----\nExpected\nTime", 
        style="bold"
    )
    table.add_column(
        "Last\nTimestamp", 
        style="bold"
    )

    data_col_str = "[green]/data[/green]"
    summ_data_col_str = "[blue]/summ_data[/blue]"
    dataset_col_str = "[red]/datasets[/red]"
    data_col_str = f"{data_col_str}"
    data_col_str = f"{data_col_str}\n-----\n"
    data_col_str = f"{data_col_str}{summ_data_col_str}"
    data_col_str = f"{data_col_str}\n-----\n{dataset_col_str}"
    data_col_str = f"{data_col_str}\n-----\nTarget\nTest\nCount"
    table.add_column(
        data_col_str,
        style="bold"
    )

    table.add_column(
        "Last\n100\nStatuses", 
        style="bold"
    )
    table.add_column(
        "Failed\nIPs", 
        style="bold"
    )

    for experiment in ongoing_info:
        experiment_name = experiment['experiment_name']
        experiment_name = experiment_name.replace(" ", "\n")
        target_test_count = experiment['target_test_count']
        data_count = experiment['data']
        datasets = experiment['datasets']
        summarised_data_count = experiment['summarised_data']
        ess_df = experiment['ess_df']

        line_break_point = 10
        ip_output, ip_dict = get_ip_output_from_ess_df(ess_df, line_break_point)

        last_timestamp = get_last_timestamp_from_ess_df(experiment['ess_df'])
        if last_timestamp is None:
            last_timestamp = "-"
        else:
            last_timestamp = last_timestamp.replace(" ", "\n")

        if summarised_data_count == "0":
            summarised_data_count = "-"

        if len(datasets) > 0:
            datasets_output = str(len(datasets))
        else:
            datasets_output = "-"

        last_n_statuses, status_dict = get_last_n_statuses_as_string_from_ess_df(ess_df, 100, line_break_point)
        status_dict_string = ""
        for status, emoji in status_dict.items():
            status_percentage = get_status_percentage_from_ess_df(ess_df, status)
            status_dict_string += f"{emoji} {status} ({status_percentage}%)\n"

        last_n_statuses = last_n_statuses + "\n\n" + status_dict_string

        if "expected_time_str" not in experiment.keys():
            expected_time_str = "-"
        else:
            expected_time_str = experiment['expected_time_str'].replace(" ", "\n")

        if "elapsed_time_str" not in experiment.keys():
            elapsed_time_str = "-"
        else:
            elapsed_time_str = experiment['elapsed_time_str'].replace(" ", "\n")

        if data_count == "0":
            data_count = "-"

        ip_dict_string = ""
        for ip, emoji in ip_dict.items():
            ip_fail_percent = get_ip_fail_percent(ip, ess_df)
            ip_dict_string += f"{emoji} {ip} ({ip_fail_percent}%)\n"

        failed_ip_output = ip_output + "\n\n" + ip_dict_string

        summarised_data_count = f"[blue]{summarised_data_count}[/blue]"
        datasets_output = f"[red]{datasets_output}[/red]"

        test_count_row_str = f"[green]{data_count}[/green]"
        test_count_row_str = f"{test_count_row_str}\n-----\n"
        test_count_row_str = f"{test_count_row_str}{summarised_data_count}"
        test_count_row_str = f"{test_count_row_str}\n-----\n{datasets_output}"
        test_count_row_str = f"{test_count_row_str}\n-----\n{target_test_count}"

        table.add_row(
            f"{experiment_name}",
            f"{elapsed_time_str}\n-----\n{expected_time_str}",
            f"{last_timestamp}",
            f"{test_count_row_str}",
            last_n_statuses,
            failed_ip_output
        )

    console.print(table)
    
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

    for MACHINE_CONFIG in CONFIG:
        console.print(Markdown(f"# {MACHINE_CONFIG['name']} ({MACHINE_CONFIG['ip']})"))

        start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ongoing_info = get_ongoing_info_from_machine(MACHINE_CONFIG)
        if ongoing_info is None:
            logger.error(
                f"Couldn't get ongoing info from {MACHINE_CONFIG['name']} ({MACHINE_CONFIG['ip']})."
            )
            continue

        display_as_table(ongoing_info)

        end_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S") - datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S")
        duration = duration.total_seconds()
        duration = int(duration)
        console.print(f"Ran in {duration} seconds.")

if __name__ == "__main__":
    main(sys.argv)
