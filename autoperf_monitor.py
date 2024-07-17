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
from rich.table import Table
from rich.console import Console
from io import StringIO

console = Console()

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd

DEBUG_MODE = False

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, 
    filename="autoperf.log", 
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
    experiment_dirname = os.path.join("data", experiment_dirname)

    return experiment_dirname

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
            logger.error(
                f"Error running {command} over SSH on {machine_name}: {stderr}"
            )
            return None

    return stdout

def get_latest_config_from_machine(machine_config: Dict = {}) -> Optional[Dict]:
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
    # TODO: Validate parameters
    # TODO: Write unit tests for this function
    # TODO: Implement this function.

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
    experiment_dirname = os.path.join("data", experiment_dirname)

    return experiment_dirname

def calculate_completed_tests_for_experiments(config: Dict = {}, machine_config: Dict = {}) -> Optional[Dict]:
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
        dir_contents = [_ for _ in dir_contents if not _.endswith(".csv") and not _.startswith("summarised_data")]
        
        experiment['completed_tests'] = dir_contents

    return config

def check_for_zip_results(config: Dict = {}, machine_config: Dict = {}) -> Optional[Dict]:
    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    get_zips_command = "ls ~/AutoPerf/data/*.zip"
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
    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    for experiment in config:
        experiment_dirname = get_dirname_from_experiment(experiment)
        if experiment_dirname is None:
            logger.error(
                f"Couldn't get experiment dirname for {experiment['experiment_name']}."
            )
            continue

        ess_filepath = os.path.join("~/AutoPerf", experiment_dirname, "ess.csv")
        check_ess_exists_command = f"ls {ess_filepath}"
        check_ess_exists_output = run_command_via_ssh(
            machine_config,
            check_ess_exists_command
        )
        if check_ess_exists_output is None:
            experiment['ess_df'] = None
            continue

        if "No such file or directory" in check_ess_exists_output:
            experiment['ess_df'] = None
            continue

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

    config_dict = json.loads(config_contents)
    if config_dict is None:
        logger.error(
            f"Couldn't parse config file from {machine_config['name']} ({machine_config['ip']})."
        )
        return None

    return config_dict

def get_ongoing_info_from_machine(machine_config: Dict = {}) -> Optional[None]:
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
    - Calculate completed tests for experiments
    - Check for zip results
    - Get ESS for experiments
    """

    with console.status(f"Getting latest config from {machine_name} ({machine_ip})...") as status:
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

        status.update(f"Counting completed tests for experiments on {machine_name} ({machine_ip})...")
        ap_config = calculate_completed_tests_for_experiments(ap_config, machine_config)
        if ap_config is None:
            logger.error(
                f"Couldn't calculate completed tests for experiments."
            )
            return

        status.update(f"Checking for zip results for experiments on {machine_name} ({machine_ip})...")
        ap_config = check_for_zip_results(ap_config, machine_config)
        if ap_config is None:
            logger.error(
                f"Couldn't check for zip results for experiments."
            )
            return

        status.update(f"Getting ESS for experiments on {machine_name} ({machine_ip})...")
        ap_config = get_ess_df_for_experiments(ap_config, machine_config)
        if ap_config is None:
            logger.error(
                f"Couldn't get ESS for experiments."
            )
            return

    return ap_config

def get_status_percentage_from_ess_df(ess_df: pd.DataFrame = None, status: str = "") -> Optional[float]:
    # TODO: Write unit tests

    if ess_df is None:
        logger.error(
            f"No ESS DataFrame passed."
        )
        return None

    if ess_df.empty:
        logger.error(
            f"ESS DataFrame is empty."
        )
        return None

    if status == "":
        logger.error(
            f"No status passed."
        )
        return None

    if status not in ["success", "fail"]:
        logger.error(
            f"Invalid status passed."
        )
        return None

    if status == "success":
        chosen_tests = ess_df[ess_df['end_status'] == status]
    else:
        chosen_tests = ess_df[ess_df['end_status'].str.contains(status)]
    chosest_test_count = len(chosen_tests.index)
    total_test_count = len(ess_df.index)

    status_percent = (chosest_test_count / total_test_count) * 100
    status_percent = round(status_percent, 1)
    return status_percent

def get_last_n_statuses_from_ess_df(ess_df: pd.DataFrame = None, n: int = 0, line_break_point: int = 20) -> Optional[str]:
    # TODO: Write unit tests

    if ess_df is None:
        logger.error(
            f"No ESS DataFrame passed."
        )
        return None

    if ess_df.empty:
        logger.error(
            f"ESS DataFrame is empty."
        )
        return None

    if n == 0:
        logger.error(
            f"No n passed."
        )
        return None

    if n < 0:
        logger.error(
            f"Invalid n passed."
        )
        return None

    if line_break_point < 0:
        logger.error(
            f"Invalid line_break_point passed."
        )
        return None

    last_n_statuses = ess_df['end_status'].tail(n)
    last_n_statuses_output = ""
    for status in last_n_statuses:
        if "success" in status:
            last_n_statuses_output += "🟢"
        else:
            last_n_statuses_output += "🔴"

    # Add a line break after every line_break_point
    last_n_statuses_output = "\n".join(
        last_n_statuses_output[i:i+line_break_point] for i in range(0, len(last_n_statuses_output), 20)
    )

    return last_n_statuses_output

def display_as_table(ongoing_info: Dict = {}) -> Optional[None]:
    # TODO: Validate parameters

    console.print(f"Legend: [green]Completed[/green], [red]Failed[/red], [white]Not Completed[/white]")

    table = Table(title="Experiments Overview", show_lines=True)
    table.add_column("Experiment Name", style="bold")
    table.add_column("Count", style="bold")
    table.add_column("Status", style="bold")
    table.add_column("Last 500 Statuses", style="bold")

    for experiment in ongoing_info:
        experiment_name = experiment['experiment_name']
        target_test_count = experiment['target_test_count']
        completed_test_count = len(experiment['completed_tests'])
        zip_results_exist = experiment['zip_results_exist']

        failed_percent = get_status_percentage_from_ess_df(experiment['ess_df'], "fail")
        succes_percent = get_status_percentage_from_ess_df(experiment['ess_df'], "success")

        last_n_statuses = get_last_n_statuses_from_ess_df(experiment['ess_df'], 500, 10)

        if zip_results_exist:
            completed_colour = "green"
        else:
            completed_colour = "white"

        table.add_row(
            f"[{completed_colour}]{experiment_name}[/{completed_colour}]",
            f"[{completed_colour}]{completed_test_count} / {target_test_count}[/{completed_colour}]",
            f"[green]{succes_percent}%[/green] [red]{failed_percent}%[/red]",
            last_n_statuses
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
        ongoing_info = get_ongoing_info_from_machine(MACHINE_CONFIG)
        if ongoing_info is None:
            logger.error(
                f"Couldn't get ongoing info from {MACHINE_CONFIG['name']} ({MACHINE_CONFIG['ip']})."
            )
            continue

        display_as_table(ongoing_info)

if __name__ == "__main__":
    if pytest.main(["-q", "./pytests", "--exitfirst"]) == 0:
        main(sys.argv)
    else:
        logger.error("Tests failed.")
        sys.exit(1)
