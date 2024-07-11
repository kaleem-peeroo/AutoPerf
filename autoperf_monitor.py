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

def get_test_name_from_combination_dict(combination_dict: Dict = {}) -> Optional[str]:
    if combination_dict == {}:
        logger.error(
            f"No combination dict passed."
        )
        return None

    if combination_dict is None:
        logger.error(
            f"Combination dict is None."
        )
        return None

    if combination_dict.keys() == []:
        logger.error(
            f"No keys found in combination dict."
        )
        return None

    if len(get_difference_between_lists(
        list(combination_dict.keys()),
        REQUIRED_QOS_KEYS
    )) > 0:
        logger.error(
            f"Invalid configuration options in combination dict."
        )
        return None

    duration_string = None
    datalen_string = None
    pub_string = None
    sub_string = None
    rel_string = None
    mc_string = None
    dur_string = None
    lc_string = None
    for key, value in combination_dict.items():
        if value == "":
            logger.error(
                f"Value for {key} is empty."
            )
            return None

        if key.lower() == "duration_secs":
            duration_string = f"{value}SEC"

        elif key.lower() == "datalen_bytes":
            datalen_string = f"{value}B"

        elif key.lower() == "pub_count":
            pub_string = f"{value}PUB"
            
        elif key.lower() == "sub_count":
            sub_string = f"{value}SUB"

        elif key.lower() == "use_reliable":

            if value:
                rel_string = "REL"
            else:
                rel_string = "BE"

        elif key.lower() == "use_multicast":
            if value:
                mc_string = "MC"
            else:
                mc_string = "UC"

        elif key.lower() == "durability_level":
            dur_string = f"{value}DUR"

        elif key.lower() == "latency_count":
            lc_string = f"{value}LC"

    if duration_string is None:
        duration_string = ""
    if datalen_string is None:
        datalen_string = ""
    if pub_string is None:
        pub_string = ""
    if sub_string is None:
        sub_string = ""
    if rel_string is None:
        rel_string = ""
    if mc_string is None:
        mc_string = ""
    if dur_string is None:
        dur_string = ""
    if lc_string is None:
        lc_string = ""

    test_name = f"{duration_string}_"
    test_name = f"{test_name}{datalen_string}_"
    test_name = f"{test_name}{pub_string}_"
    test_name = f"{test_name}{sub_string}_"
    test_name = f"{test_name}{rel_string}_"
    test_name = f"{test_name}{mc_string}_"
    test_name = f"{test_name}{dur_string}_"
    test_name = f"{test_name}{lc_string}"

    return test_name

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

def have_last_n_tests_failed(ess_df: pd.DataFrame, n: int = 10) -> Optional[bool]:
    if ess_df is None:
        logger.error(
            f"No ESS dataframe passed."
        )
        return None

    if n == 0:
        logger.warning(
            f"Can't check last 0 tests."
        )
        return False

    if n < 0:
        logger.error(
            f"Can't check negative number of tests: {n}."
        )
        return None

    if len(ess_df.index) == 0:
        logger.warning(
            f"ESS dataframe is empty."
        )
        return False

    if len(ess_df.index) < n:
        logger.warning(
            f"ESS dataframe has less than {n} tests."
        )
        n = len(ess_df.index)

    last_n_tests = ess_df.tail(n)
    if last_n_tests is None:
        logger.error(
           f"Couldn't get the last {n} tests."
        )
        return None

    failed_tests = last_n_tests[last_n_tests['end_status'] != 'success']
    if len(failed_tests.index) == n:
        return True

    return False

def get_buffer_duration_secs_from_test_duration_secs(test_duration_secs: int = 0) -> Optional[int]:
    if test_duration_secs == 0:
        logger.error(
            f"Test duration is 0."
        )
        return None

    if test_duration_secs < 0:
        logger.error(
            f"Test duration is negative."
        )
        return None

    buffer_duration_sec = test_duration_secs * 0.05

    if buffer_duration_sec < 30:
        buffer_duration_sec = 30

    return int(buffer_duration_sec)

def has_failures_in_machine_statuses(machine_statuses) -> Optional[bool]:
    if machine_statuses == {}:
        logger.error(
            f"No machine statuses passed."
        )
        return None

    if len(machine_statuses.keys()) == 0:
        logger.error(
            f"No keys in machine statuses."
        )
        return None

    for _, status in machine_statuses.items():
        if status != "complete" and status != "pending" and status != "results downloaded":
            return True

    return False

def update_machine_status(machine_statuses: Dict = {}, machine_ip: str = "", new_status: str = "") -> Optional[Dict]:
    if machine_statuses == {}:
        logger.error(
            f"No machine statuses passed."
        )
        return None

    if machine_ip == "":
        logger.error(
            f"No machine IP passed."
        )
        return None

    if new_status == "":
        logger.error(
            f"No new status passed."
        )
        return None

    if machine_ip not in machine_statuses.keys():
        logger.error(
            f"Machine IP not found in machine statuses."
        )
        return None

    machine_statuses[machine_ip] = new_status

    return machine_statuses

def download_results_from_machine(machine_config, machine_statuses, local_results_dirpath):
    if machine_config == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

    if machine_statuses == {}:
        logger.error(
            f"No machine statuses passed."
        )
        return None

    if local_results_dirpath == "":
        logger.error(
            f"No local results dirpath passed."
        )
        return None

    if has_failures_in_machine_statuses(machine_statuses):
        logger.error(
            f"Machine statuses have failures:"
        )
        logger.error(
            f"{machine_statuses}"
        )
        return None

    machine_name = machine_config['machine_name']
    machine_ip = machine_config['ip']

    logger.debug(
        f"Downloading results from {machine_name} ({machine_ip})."
    )

    username = machine_config['username']
    perftest_exec_path = machine_config['perftest_exec_path']
    results_dir = os.path.dirname(perftest_exec_path)

    check_for_csv_command = f"ssh {username}@{machine_ip} 'ls {results_dir}/*.csv'"
    check_for_csv_process = subprocess.Popen(
        check_for_csv_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = check_for_csv_process.communicate(timeout=30)
    stdout = stdout.decode('utf-8').strip()
    stderr = stderr.decode('utf-8').strip()

    if check_for_csv_process.returncode != 0:
        logger.error(
            f"Error checking for CSV files on {machine_name}: {stderr}"
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: couldn't check for CSV files"
        )
        return None

    if stdout == "":
        logger.error(
            f"No CSV files found on {machine_name}."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no CSV files found"
        )
        return None

    csv_files = stdout.split()
    csv_file_count = len(csv_files)

    if csv_file_count == 0:
        logger.error(
            f"No CSV files found on {machine_name}."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no CSV files found"
        )
        return None

    os.makedirs(local_results_dirpath, exist_ok=True)

    for csv_filepath in csv_files:
        csv_file = os.path.basename(csv_filepath)
        remote_csv_filepath = os.path.join(results_dir, csv_filepath)
        local_csv_filepath = os.path.join(local_results_dirpath, csv_file)
        logger.debug(
            # f"{machine_name}: Downloading...\n\t{remote_csv_filepath} \n\tto \n\t{local_csv_filepath}"
            f"{machine_name}: Downloading {csv_file}..."
        )
        download_command = f"scp {username}@{machine_ip}:{remote_csv_filepath} {local_csv_filepath}"
        download_process = subprocess.Popen(
            download_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = download_process.communicate(timeout=30)
        stdout = stdout.decode('utf-8').strip()
        stderr = stderr.decode('utf-8').strip()

        if download_process.returncode != 0:
            logger.error(
                f"Error downloading {csv_file} from {machine_name}: {stderr}"
            )
            update_machine_status(
                machine_statuses,
                machine_config['ip'],
                "error: couldn't download CSV files"
            )
            return None 


    delete_all_csv_command = f"ssh {username}@{machine_ip} 'rm {results_dir}/*.csv'"
    delete_all_csv_process = subprocess.Popen(
        delete_all_csv_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    stdout, stderr = delete_all_csv_process.communicate(timeout=30)
    stdout = stdout.decode('utf-8').strip()
    stderr = stderr.decode('utf-8').strip()

    if delete_all_csv_process.returncode != 0:
        logger.warning(
            f"Couldn't delete CSV files on {machine_name}. Relying on next pre-test deletion: {stderr}"
        )

    update_machine_status(
        machine_statuses,
        machine_config['ip'],
        "results downloaded"
    )

    logger.debug(
        f"Results downloaded from {machine_name} ({machine_ip})."
    )

def delete_csvs_from_machines(machine_config):
    machine_ip = machine_config['ip']
    machine_name = machine_config['machine_name']
    username = machine_config['username']
    perftest_exec_path = machine_config['perftest_exec_path']
    results_dir = os.path.dirname(perftest_exec_path)

    delete_all_csv_command = f"ssh {username}@{machine_ip} 'rm -rf {results_dir}/*.csv'"
    delete_all_csv_process = subprocess.Popen(
        delete_all_csv_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    try:
        stdout, stderr = delete_all_csv_process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        logger.error(
            f"Timed out deleting CSV files from {machine_name}."
        )
        delete_all_csv_process.kill()
        stdout, stderr = delete_all_csv_process.communicate()

    stdout = stdout.decode('utf-8').strip()
    stderr = stderr.decode('utf-8').strip()

    if delete_all_csv_process.returncode != 0:
        logger.error(
            f"Error deleting csv files from {machine_name}: {stderr}"
        )
        return None

def generate_test_config_from_qos(qos: Optional[Dict] = None) -> Optional[Dict]:
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

    test_config = {}
    for qos_setting, qos_values in qos.items():
        if len(qos_values) == 1:
            test_config[qos_setting] = qos_values[0]
            continue

        if len(qos_values) > 2:
            logger.error(f"Can't have more than 2 values for RCG qos setting: {qos_setting}")
            return None

        all_items_are_ints = all(isinstance(qos_value, int) for qos_value in qos_values)

        if all_items_are_ints:
            lower_bound = min(qos_values)
            upper_bound = max(qos_values)

            random_int_value = random.randint(lower_bound, upper_bound)

            test_config[qos_setting] = random_int_value

        else:
            test_config[qos_setting] = random.choice(qos_values)
        
    return test_config

def get_csv_file_count_from_dir(dirpath: str = "") -> Optional[int]:
    # TODO: Validate parameters
    # TODO: Implement unit tests for this function

    files_in_dir = os.listdir(dirpath)
    csv_files_in_dir = [_ for _ in files_in_dir if _.endswith(".csv")]

    return len(csv_files_in_dir)

def get_expected_csv_file_count_from_test_name(test_name: str = "") -> Optional[int]:
    # TODO: Validate parameters
    # TODO: Implement unit tests for this function

    sub_count_from_name = int(test_name.split("SUB_")[0].split("_")[-1])

    return sub_count_from_name + 1

def get_pub_df_from_pub_0_filepath(pub_file: str = "") -> Optional[pd.DataFrame]:
    # TODO: Validate parameters

    # ? Find out where to start parsing the file from 
    with open(pub_file, "r") as pub_file_obj:
        pub_first_5_lines = []
        for i in range(5):
            line = pub_file_obj.readline()
            if not line:
                break
            pub_first_5_lines.append(line)
    
    start_index = 0    
    for i, line in enumerate(pub_first_5_lines):
        if "Ave" in line and "Length (Bytes)" in line:
            start_index = i
            break

    if start_index == 0:
        logger.error(
            f"Couldn't find start index for header row for {pub_file}."
        )
        return None

    # ? Find out where to stop parsing the file from (ignore the summary stats at the end)
    with open(pub_file, "r") as pub_file_obj:
        pub_file_contents = pub_file_obj.readlines()

    pub_last_5_lines = pub_file_contents[-5:]
    line_count = len(pub_file_contents)
    
    end_index = 0
    for i, line in enumerate(pub_last_5_lines):
        if "latency summary" in line.lower():
            end_index = line_count - 5 + i - 2
            break
    
    if end_index == 0:
        # console.print(f"Couldn't find end index for {pub_file}.", style="bold red")
        logger.error(
            f"Couldn't find end index for summary row for {pub_file}."
        )
        return None

    try:
        lat_df = pd.read_csv(pub_file, skiprows=start_index, nrows=end_index-start_index, on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        # console.print(f"EmptyDataError for {pub_file}.", style="bold red")
        logger.error(
            f"EmptyDataError for {pub_file}."
        )
        return None
    
        # ? Pick out the latency column ONLY
    latency_col = None
    for col in lat_df.columns:
        if "latency" in col.lower():
            latency_col = col
            break

    if latency_col is None:
        # console.print(f"Couldn't find latency column for {pub_file}.", style="bold red")
        logger.error(
            f"Couldn't find latency column for {pub_file}."
        )
        return None

    lat_df = lat_df[latency_col]
    lat_df = lat_df.rename("latency_us")
    
    return lat_df

def get_subs_df_from_sub_files(sub_files: [str] = []) -> Optional[pd.DataFrame]:
    test_df = pd.DataFrame()
    
    for file in sub_files:
        # ? Find out where to start parsing the file from 
        with open(file, "r") as file_obj:
            if os.stat(file).st_size == 0:
                continue
            file_obj.seek(0)
            pub_first_5_lines = [next(file_obj) for x in range(5)]
            
        start_index = 0    
        for i, line in enumerate(pub_first_5_lines):
            if "Length (Bytes)" in line:
                start_index = i
                break
        
        if start_index == 0:
            # print(f"Couldn't get start_index for header row from {file}.")
            logger.warning(
                f"Couldn't get start_index for header row from {file}."
            )
            continue

        # ? Find out where to stop parsing the file from (ignore the summary stats at the end)
        with open(file, "r") as file_obj:
            file_contents = file_obj.readlines()
        pub_last_5_lines = file_contents[-5:]
        line_count = len(file_contents)
        
        end_index = 0
        for i, line in enumerate(pub_last_5_lines):
            if "throughput summary" in line.lower():
                end_index = line_count - 5 + i - 2
                break
            
        if end_index == 0:
            # print(f"Couldn't get end_index for summary row from {file}.")
            logger.warning(
                f"Couldn't get end_index for summary row from {file}."
            )
            continue

        nrows = end_index - start_index
        nrows = 0 if nrows < 0 else nrows

        try:
            df = pd.read_csv(file, on_bad_lines="skip", skiprows=start_index, nrows=nrows)
        except pd.errors.ParserError as e:
            logger.warning(
                f"Error when getting data from {file}:{e}"
            )
            continue
        
        desired_metrics = ["total samples", "samples/s", "mbps", "lost samples"]
        
        sub_name = os.path.basename(file).replace(".csv", "")

        for col in df.columns:
            for desired_metric in desired_metrics:
                if desired_metric in col.lower() and "avg" not in col.lower():
                    col_name = col.strip().lower().replace(" ", "_")
                    if "samples/s" in col_name:
                        col_name = "samples_per_sec"
                    elif "%" in col_name:
                        col_name = "lost_samples_percent"
                    test_df[f"{sub_name}_{col_name}"] = df[col]

        # ? Remove rows with strings in them
        test_df = test_df[test_df.applymap(lambda x: not isinstance(x, str)).all(1)]

        test_df = test_df.astype(float, errors="ignore")

    return test_df

def summarise_tests(dirpath: str = "") -> Optional[str]:
    if dirpath == "":
        logger.error(
            f"No dirpath passed."
        )
        return None

    summaries_dirpath = os.path.join(dirpath, "summarised_data")
    if os.path.exists(summaries_dirpath):
        summaries_dirpath_files = os.listdir(summaries_dirpath)
        if len(summaries_dirpath_files) > 0:
            logger.error(
                f"Summarised data {summaries_dirpath} already exists."
            )
            return None

    os.makedirs(sumaries_dirpath, exist_ok=True)

    test_dirpaths = [os.path.join(dirpath, _) for _ in os.listdir(dirpath)]
    test_dirpaths = [_ for _ in test_dirpaths if os.path.isdir(_)]
    test_dirpaths = [_ for _ in test_dirpaths if "summarised_data" not in _.lower()]
    if len(test_dirpaths) == 0:
        logger.warning("Found no test folders in {dirpath}")
        return None

    logger.debug(
        f"Summarising {len(test_dirpaths)} tests..."
    )

    summarised_test_count = 0
    for test_dirpath in test_dirpaths:
        test_index = test_dirpaths.index(test_dirpath)
        test_name = os.path.basename(test_dirpath)

        expected_csv_file_count = get_expected_csv_file_count_from_test_name(test_name)
        actual_csv_file_count = get_csv_file_count_from_dir(test_dirpath)

        if expected_csv_file_count != actual_csv_file_count:
            logger.warning(
                f"Skipping {test_name} because its missing {expect_csv_file_count - actual_csv_file_count} files"
            )
            continue

        csv_files = [os.path.join(test_dirpath, _) for _ in os.listdir(test_dirpath)]
        csv_files = [_ for _ in csv_files if _.endswith("csv")]

        pub_csv_files = [_ for _ in csv_files if os.path.basename(_).startswith("pub_")]

        pub_0_filepath = [_ for _ in pub_csv_files if _.endswith("0.csv")][0]
        pub_df = get_pub_df_from_pub_0_filepath(pub_0_filepath)
        if pub_df is None:
            logger.error(f"Couldn't get pub_df for {pub_0_filepath}")
            continue

        sub_csv_files = [_ for _ in csv_files if os.path.basename(_).startswith("sub_")]
        subs_df = get_subs_df_from_sub_files(sub_csv_files)
        
        df_list = [pub_df, subs_df]
        df = pd.concat(df_list, axis=1)
        df.to_csv(
            os.path.join(
                summaries_dirpath,
                f"{test_name}.csv"
            ),
            index=False
        )

        logger.debug(f"[{test_index + 1}/{len(test_dirpaths)}] Summarised {test_name}.csv")
        summarised_test_count += 1

    logger.debug(f"Summarised {summarised_test_count}/{len(test_dirpaths)} tests...")

def generate_dataset(dirpath: str = "", truncation_percent: int = 0):
    """
    Reduce each csv file in summarised_data folder to a single row in one csv file.
    """
    # TODO: Validate parameters
    # TODO: Write unit tests for this function

    summaries_dirpath = os.path.join(dirpath, "summarised_data")
    if not os.path.join(summaries_dirpath):
        logger.error(
            f"Summarised dirpath {summaries_dirpath} does NOT exist."
        )
        return None

    test_csvs = [os.path.join(summaries_dirpath, _) for _ in os.listdir(summaries_dirpath)]
    if len(test_csvs) == 0:
        logger.error(
            f"No csv files found in {summaries_dirpath}."
        )
    
    logger.info(
        f"Generating dataset from {len(test_csvs)} tests with {truncation_percent}% truncation..."
    )

    experiment_name = os.path.basename(dirpath)
    current_timestamp = datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"{current_timestamp}_{experiment_name}_dataset_{truncation_percent}_percent_truncation.csv"
    filename = os.path.join("datasets", filename)

    os.makedirs("datasets", exist_ok = True)

    dataset_df = pd.DataFrame()
    for test_csv in track(test_csvs, description="Processing..."):
        new_dataset_row = {}

        test_df = pd.read_csv(test_csv)
        test_name = os.path.basename(test_csv)

        new_dataset_row = get_qos_dict_from_test_name(test_name)
        if new_dataset_row is None:
            logger.error(
                f"Couldn't get qos dict for {test_name}."
            )
            continue

        for column in test_df.columns:
            column_values = test_df[column]
            value_count = len(column_values)

            values_to_truncate = int(
                truncation_percent * (100 / value_count)
            )

            column_values = test_df[column].iloc[values_to_truncate:]
            column_df = pd.DataFrame(column_values)
            
            for PERCENTILE in PERCENTILES:
                new_dataset_row[f"{column}_{PERCENTILE}%"] = column_df.quantile(PERCENTILE / 100)

        new_dataset_row_df = pd.DataFrame(
            [new_dataset_row]
        )

        dataset_df = pd.concat(
            [dataset_df, new_dataset_row_df],
            axis = 0,
            ignore_index = True
        )

    dataset_df.to_csv(filename, index=False)
    logger.info(
        f"Dataset writtent to {filename}"
    )

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

def calculate_pcg_target_test_count(experiment_config: Dict = {}) -> Optional[int]:
    # TODO: Validate parameters
    # TODO: Write unit tests for this function
    # TODO: Implement this function.

    qos = experiment_config['qos']
    pprint(experiment_config)

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
        ess_df = pd.read_csv(ess_file_contents)
        
        experiment['ess_df'] = ess_df

    return config

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
        ap_config = get_latest_config_from_machine(machine_config)
        if ap_config is None:
            logger.error(
                f"Couldn't get latest config from {machine_name} ({machine_ip})."
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

def display_experiments_overview_table(ongoing_info: Dict = {}) -> Optional[None]:
    # TODO: Validate parameters

    table = Table(title="Experiments Overview")
    table.add_column("Experiment Name", style="bold")
    table.add_column("Count", style="bold")

    for experiment in ongoing_info:
        experiment_name = experiment['experiment_name']
        target_test_count = experiment['target_test_count']
        completed_test_count = len(experiment['completed_tests'])
        zip_results_exist = experiment['zip_results_exist']

        if zip_results_exist:
            zip_colour = "green"
        else:
            zip_colour = "white"

        table.add_row(
            experiment_name,
            f"[{zip_colour}]{completed_test_count} / {target_test_count}[/{zip_colour}]"
        )

    console.print(table)
        
def display_experiments_details_table(ongoing_info: Dict = {}) -> Optional[None]:
    # TODO: Validate parameters

    return

def display_as_table(ongoing_info: Dict = {}) -> Optional[None]:
    # TODO: Validate parameters
    
    display_experiments_overview_table(ongoing_info)

    display_experiments_details_table(ongoing_info)
    
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
