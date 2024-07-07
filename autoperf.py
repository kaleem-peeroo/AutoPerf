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

from icecream import ic
from typing import Dict, List, Optional
from pprint import pprint
from multiprocessing import Process, Manager

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd

DEBUG_MODE = True
SKIP_RESTART = True

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

REQUIRED_EXPERIMENT_KEYS = [
    'experiment_name',
    'combination_generation_type',
    'qos_settings',
    'slave_machines',
    'rcg_target_test_count'
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

def ping_machine(ip: str = "") -> Optional[bool]:
    if ip == "":
        logger.error(
            f"No IP passed for connection check."
        )
        return None

    logger.debug(
        f"Pinging {ip}"
    )

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

    logger.debug(
        f"Checking SSH connection to {username}@{ip}"
    )

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
    experiment_dirname = os.path.join("data", experiment_dirname)

    return experiment_dirname

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
            "qos_settings",
            "comments"
       ])

    return ess_df

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

def get_next_test_from_ess(ess_df: pd.DataFrame) -> Optional[Dict]:
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

def generate_scripts_from_qos_config(qos_config: Dict = {}) -> Optional[List]:
    if qos_config == {}:
        logger.error(
            f"No QoS config passed."
        )
        return None

    if not isinstance(qos_config, dict):
        logger.error(
            f"QoS config is not a dictionary."
        )
        return None

    qos_keys = qos_config.keys()
    if len(qos_keys) == 0:
        logger.error(
            f"No keys found in QoS config."
        )
        return None

    for key in qos_keys:
        if key not in REQUIRED_QOS_KEYS:
            logger.error(
                f"Invalid QoS setting: {key}"
            )
            return None

    qos_values = qos_config.values()
    if len(qos_values) == 0:
        logger.error(
            f"No values found in QoS config."
        )
        return None

    for value in qos_values:
        if value == "":
            logger.error(
                f"Empty value found in QoS config."
            )
            return None

    data_len_str = f"-dataLen {qos_config['datalen_bytes']}"
    durability_str = f"-durability {qos_config['durability_level']}"
    latency_count_str = f"-latencyCount {qos_config['latency_count']}"
    exec_time_str = f"-executionTime {qos_config['duration_secs']}"

    mc_str = ""
    if qos_config['use_multicast']:
        mc_str = "-multicast"

    rel_str = ""
    if not qos_config['use_reliable']:
        rel_str = "-bestEffort"

    script_base = data_len_str + " "
    if rel_str != "":
        script_base = script_base + rel_str + " "
    if mc_str != "":
        script_base = script_base + mc_str + " "
    script_base = script_base + durability_str

    script_bases = []

    pub_count = qos_config['pub_count']
    if pub_count == 0:
        logger.error(
            f"Pub count is 0 in generate_scripts_from_qos_config()"
        )
        return None
    sub_count = qos_config['sub_count']
    if sub_count == 0:
        logger.error(
            f"Sub count is 0 in generate_scripts_from_qos_config()"
        )
        return None

    if pub_count == 1:
        pub_script = f"{script_base} -pub -outputFile pub_0.csv"
        pub_script = pub_script + f" -numSubscribers {sub_count}"
        pub_script = pub_script + f" {exec_time_str}"
        pub_script = pub_script + f" {latency_count_str}"
        pub_script = pub_script + f" -batchSize 0"

        script_bases.append(pub_script)
    else:
        for i in range(pub_count):
            # Define the output file for the first publisher.
            if i == 0:
                pub_script = f"{script_base} -pub"
                pub_script = pub_script + f" -pidMultiPubTest {i}"
                pub_script = pub_script + f" -outputFile pub_{i}.csv"
                pub_script = pub_script + f" -numSubscribers {sub_count}"
                pub_script = pub_script + f" {exec_time_str}"
                pub_script = pub_script + f" {latency_count_str}"
                pub_script = pub_script + f" -batchSize 0"

                script_bases.append(pub_script)
            else:
                pub_script = f"{script_base} -pub"
                pub_script = pub_script + f" -pidMultiPubTest {i}"
                pub_script = pub_script + f" -numSubscribers {sub_count}"
                pub_script = pub_script + f" {exec_time_str}"
                pub_script = pub_script + f" {latency_count_str}"
                pub_script = pub_script + f" -batchSize 0"

                script_bases.append(pub_script)
    
    if sub_count == 1:
        sub_script = f"{script_base} -sub -outputFile sub_0.csv"
        sub_script = sub_script + f" -numPublishers {pub_count}"

        script_bases.append(sub_script)
    else:
        for i in range(sub_count):
            sub_script = f"{script_base} -sub"
            sub_script = sub_script + f" -sidMultiSubTest {i}"
            sub_script = sub_script + f" -outputFile sub_{i}.csv"
            sub_script = sub_script + f" -numPublishers {pub_count}"

            script_bases.append(sub_script)

    scripts = []
    for script_base in script_bases:
        script = f"{script_base} -transport UDPv4"
        scripts.append(script)

    return scripts
    
def get_machines_by_type(machine_configs: List = [], machine_type: str = "") -> Optional[List]:
    if machine_configs == []:
        logger.error(
            f"No machine configs passed."
        )
        return None

    if machine_type == "":
        logger.error(
            f"No machine type passed."
        )
        return None

    if machine_type not in ['pub', 'sub']:
        logger.error(
            f"Invalid machine type: {machine_type}"
        )
        return None

    machines_to_return = []
    for machine_config in machine_configs:
        if machine_config['participant_allocation'] in [machine_type, 'all']:
            machines_to_return.append(machine_config)

    return machines_to_return

def distribute_scripts_to_machines(scripts: List = [], machine_configs: List = []) -> Optional[List]:
    if scripts == []:
        logger.error(
            f"No scripts passed."
        )
        return None

    if machine_configs == []:
        logger.error(
            f"No machine configs passed."
        )
        return None

    pub_scripts = [script for script in scripts if "-pub" in script]
    if len(pub_scripts) == 0:
        logger.error(
            f"No publisher scripts found."
        )
        return None

    sub_scripts = [script for script in scripts if "-sub" in script]
    if len(sub_scripts) == 0:
        logger.error(
            f"No subscriber scripts found."
        )
        return None

    pub_machines = get_machines_by_type(machine_configs, 'pub')
    if pub_machines == []:
        logger.error(
            f"No publisher machines found."
        )
        return None

    sub_machines = get_machines_by_type(machine_configs, 'sub')
    if sub_machines == []:
        logger.error(
            f"No subscriber machines found."
        )
        return None

    # Add 'source ~/.bashrc; cd /path/to/executable;' to all machines first.
    for machine in machine_configs:
        perftest_exec_path = machine['perftest_exec_path']
        perftest_exec_dirpath = os.path.dirname(perftest_exec_path)

        machine['script'] = f'source ~/.bashrc; cd {perftest_exec_dirpath}; '

    for i, pub_script in enumerate(pub_scripts):
        script = pub_script
        machine = pub_machines[i % len(pub_machines)]
        
        perftest_exec_basename = os.path.basename(machine['perftest_exec_path'])
        perftest_exec_basename = f"./{perftest_exec_basename}"

        machine['script'] = f"{machine['script']} & {perftest_exec_basename} {script}"
        
    for i, sub_script in enumerate(sub_scripts):
        script = sub_script
        """
        Distribute the subscriber scripts in reverse order.
        This evens out the allocation across machines.
        Example:
        Without reverse:
            machine 1: p1, p3, s1, s3
            machine 2: p2, s2
        With reverse:
            machine 1: p1, p3, s2
            machine 2: p2, s1, s3
        """
        machine_index = len(sub_machines) - 1 - (i % len(sub_machines))
        machine = sub_machines[machine_index]
        
        perftest_exec_basename = os.path.basename(machine['perftest_exec_path'])
        perftest_exec_basename = f"./{perftest_exec_basename}"

        machine['script'] = f"{machine['script']} & {perftest_exec_basename} {script}"

    for machine in machine_configs:
        # Remove the first ' & ' from each script.
        machine['script'] = machine['script'].replace(' & ', '', 1)
        # Add a ' & ' to the end of each script.
        machine['script'] = f"{machine['script']} &"

    return machine_configs

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

def run_script_on_machine(
        machine_config: Dict = {}, 
        machine_statuses: Dict = {}, 
        timeout_secs: int = 0
    ) -> None:

    if machine_config == {}:
        logger.error(
            f"No machine config passed."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no machine config passed"
        )
        return None

    if machine_statuses == {}:
        logger.error(
            f"No machine statuses passed."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no machine statuses passed"
        )
        return None

    if timeout_secs == 0:
        logger.error(
            f"No timeout passed to run_script_on_machine()."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no timeout passed"
        )
        return None

    if has_failures_in_machine_statuses(machine_statuses):
        logger.error(
            f"Machine statuses have failures:"
        )
        logger.error(
            f"{machine_statuses}"
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: machine statuses have failures"
        )
        return None

    logger.debug(
        f"Running script on {machine_config['machine_name']} ({machine_config['ip']})."
    )

    script_string = machine_config['script']
    machine_ip = machine_config['ip']
    username = machine_config['username']
    ssh_command = f"ssh {username}@{machine_ip} '{script_string}'"

    process = subprocess.Popen(
        ssh_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    try:
        stdout, stderr = process.communicate(timeout=timeout_secs)
        stdout = stdout.decode('utf-8').strip()
        stderr = stderr.decode('utf-8').strip()

        return_code = process.returncode

        if return_code != 0:
            logger.error(
                f"Error running script on {machine_config['machine_name']}."
            )
            logger.error(
                f"Return code: \t{return_code}"
            )
            if stdout != "":
                logger.error(
                    f"stdout: \t{stdout}"
                )
            if stderr != "":
                logger.error(
                    f"stderr: \t{stderr}"
                )

            update_machine_status(
                machine_statuses,
                machine_config['ip'],
                "error: return code not 0"
            )
        else:
            logger.debug(
                f"Script on {machine_config['machine_name']} ran successfully."
            )
            update_machine_status(
                machine_statuses,
                machine_config['ip'],
                "complete"
            )

    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate()
        stdout = stdout.decode('utf-8').strip()
        stderr = stderr.decode('utf-8').strip()

        logger.error(
            f"Script on {machine_config['machine_name']} timed out."
        )
        if stdout != "":
            logger.error(
                f"stdout: \t\t{stdout}"
            )

        if stderr != "":
            logger.error(
                f"stderr: \t\t{stderr}"
            )

        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            f"error: timed out after {timeout_secs} seconds"
        )

    return None

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
    stdout, stderr = check_for_csv_process.communicate(timeout=60)
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
        stdout, stderr = download_process.communicate(timeout=60)
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
    stdout, stderr = delete_all_csv_process.communicate(timeout=60)
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
    stdout, stderr = delete_all_csv_process.communicate(timeout=60)
    stdout = stdout.decode('utf-8').strip()
    stderr = stderr.decode('utf-8').strip()

    if delete_all_csv_process.returncode != 0:
        logger.error(
            f"Error deleting csv files from {machine_name}: {stderr}"
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: couldn't delete CSV files before test"
        )
        return None
def update_ess_df(
    ess_df: pd.DataFrame = pd.DataFrame(),
    start_timestamp: str = None,
    end_timestamp: str = None,
    test_name: str = "",
    ping_count: int = 0,
    ssh_check_count: int = 0,
    end_status: str = "",
    qos_settings: Dict = {},
    comments: str = ""
):
    new_ess_row = {}
    new_ess_row['start_timestamp'] = start_timestamp
    new_ess_row['end_timestamp'] = end_timestamp
    new_ess_row['test_name'] = test_name
    new_ess_row['ping_count'] = ping_count
    new_ess_row['ssh_check_count'] = ssh_check_count
    new_ess_row['end_status'] = end_status
    new_ess_row['qos_settings'] = qos_settings
    new_ess_row['comments'] = comments

    new_ess_row_df = pd.DataFrame([new_ess_row])
    new_ess_df = pd.concat(
        [ess_df, new_ess_row_df],
        axis = 0,
        ignore_index = True
    )
    return new_ess_df

                    
def run_test(
    test_config: Dict = {}, 
    machine_configs: List = [],
    ess_df: pd.DataFrame = pd.DataFrame(),
    experiment_dirpath: str = ""
) -> Optional[pd.DataFrame]:
    if test_config == {}:
        logger.error(
            f"No test config passed."
        )
        return None

    if machine_configs == []:
        logger.error(
            f"No machine config passed."
        )
        return None

    if ess_df is None:
        logger.error(
            f"No ESS dataframe passed."
        )
        return None

    if not isinstance(ess_df, pd.DataFrame):
        logger.error(
            f"ESS dataframe is not a dataframe."
        )
        return None

    if not isinstance(test_config, dict):
        logger.error(
            f"Next test config is not a dictionary."
        )
        return None

    if not isinstance(machine_configs, List):
        logger.error(
            f"Machine config is not a list."
        )
        return None

    if experiment_dirpath == "":
        logger.error(
            f"No experiment dirpath passed."
        )
        return None

    new_ess_df = ess_df

    test_name = get_test_name_from_combination_dict(test_config)
    if test_name is None:
        logger.error(
            f"Couldn't get the name of the next test to run."
        )
        return None

    """
    1. Check connections to machines.
    2. Restart machines.
    3. Check connections to machines.
    4. Get qos config.
    5. Generate scripts.
    6. Allocate scripts to machines.
    7. Run scripts.
    8. Check results.
    9. Update ESS.
    10. Return ESS.
    """

    new_ess_row = {}
    new_ess_row['comments'] = ""

    if not DEBUG_MODE:
        # 1. Check connections to machines.
        for machine_config in machine_configs:
            machine_ip = machine_config['ip']
            if not ping_machine(machine_ip):
                logger.error(
                    f"Couldn't ping {machine_ip}."
                )
                return update_ess_df(
                    new_ess_df,
                    None,
                    None,
                    test_name,
                    0,
                    0,
                    "failed initial ping check",
                    test_config,
                    new_ess_row['comments'] + f"Failed to even ping {machine_ip} the first time."
                )


            if not check_ssh_connection(
                machine_config
            ):
                logger.error(
                    f"Couldn't SSH into {machine_ip}."
                )
                return update_ess_df(
                    new_ess_df,
                    None,
                    None,
                    test_name,
                    1,
                    0,
                    "failed initial ssh check",
                    test_config,
                    new_ess_row['comments'] + f"Failed to even ssh {machine_ip} the first time after pinging."
                )

        
        logger.debug(f"Restarting all machines")
        
        # 2. Restart machines.
        if not SKIP_RESTART:
            for machine_config in machine_configs:
                logger.debug(
                    f"Restarting {machine_config['machine_name']}..."
                )
                machine_ip = machine_config['ip']
                restart_command = f"ssh -i {machine_config['ssh_key_path']} {machine_config['username']}@{machine_ip} 'sudo reboot'"
                with open(os.devnull, 'w') as devnull:
                    subprocess.run(
                        restart_command, 
                        shell=True, 
                        stdout=devnull, 
                        stderr=devnull
                    )
            
            logger.debug(
                f"All machines have been restarted. Waiting 15 seconds..."
            )
            
            time.sleep(15)
            
        # 3. Check connections to machines.
        ping_count = 0
        ssh_check_count = 0
        for machine_config in machine_configs:
            machine_ip = machine_config['ip']
            machine_name = machine_config['machine_name']
           
            # Ping machine up to 5 times.
            for attempt in range(1, 6):
                if ping_machine(machine_ip):
                    ping_count = attempt
                    break
                        
                time.sleep(3)
        
            # SSH into machine up to 5 times.
            for attempt in range(1, 6):
                if check_ssh_connection(
                    machine_config
                ):
                    ssh_check_count = attempt
                    break
        
                time.sleep(1)
        
        if ping_count == 5 and ssh_check_count == 5:
            logger.warning(f"Machines failed connection checks.")

            return update_ess_df(
                new_ess_df,
                None,
                None,
                test_name,
                ping_count,
                ssh_check_count,
                "failed connection checks",
                test_config,
                new_ess_row['comments'] + f"Failed connection check after 5 pings and ssh checks."
            )
            
        else:
            logger.debug(
                f"All machines are up and running."
            )

    if DEBUG_MODE:
        ping_count = 0
        ssh_check_count = 0

    # 4. Get qos config.
    qos_config = test_config

    # 5. Generate scripts.
    scripts = generate_scripts_from_qos_config(
        qos_config
    )

    if scripts is None:
        logger.error(
            f"Error generating scripts from: \n\t{qos_config}"
        )
        return update_ess_df(
            new_ess_df,
            None,
            None,
            test_name,
            ping_count,
            ssh_check_count,
            "failed script generation",
            qos_config,
            new_ess_row['comments'] + " Failed to generate scripts from qos config."
        )

    # 6. Allocate scripts to machines.
    scripts_per_machine = distribute_scripts_to_machines(
        scripts,
        machine_configs
    )
    if scripts_per_machine is None:
        logger.error(f"Error distributing scripts to machines.")
        return update_ess_df(
            new_ess_df,
            None,
            None,
            test_name,
            ping_count,
            ssh_check_count,
            "failed script distribution",
            qos_config,
            new_ess_row['comments'] + " Failed to distribute scripts across machines."
        )

    if len(scripts_per_machine) == 0:
        logger.error(f"No scripts allocated to machines.")
        return update_ess_df(
            new_ess_df,
            None,
            None,
            test_name,
            ping_count,
            ssh_check_count,
            "failed script distribution",
            qos_config,
            new_ess_row['comments'] + " No scripts allocated to machines."
        )

    # 6.1. Delete any old .csv files from previous tests.
    logger.debug(f"Deleting csv files before test...")
    with Manager() as manager:
        processes = []
        for machine_config in scripts_per_machine:
            process = Process(
                target=delete_csvs_from_machines,
                args=(
                    machine_config,
                )
            )
            processes.append(process)
            process.start()

        for process in processes:
            process.join(60)
            if process.is_alive():
                logger.error(
                    f"Process for deleting old .csv files timed out after 60 seconds. Terminating..."
                )
                process.terminate()
                process.join()

    # 7. Run scripts.

    test_duration_secs = qos_config['duration_secs']
    buffer_duration_secs = get_buffer_duration_secs_from_test_duration_secs(
        test_duration_secs
    )
    timeout_secs = test_duration_secs + buffer_duration_secs

    # Start Timestamp
    start_timestamp = datetime.datetime.now()

    with Manager() as manager:
        machine_statuses = manager.dict()

        # Initialise machine statuses with IP and "pending"
        for machine_config in scripts_per_machine:
            machine_statuses[machine_config['ip']] = "pending"

        processes = []
        for machine_config in scripts_per_machine:
            machine_ip = machine_config['ip']

            process = Process(
                target=run_script_on_machine,
                args=(
                    machine_config,
                    machine_statuses,
                    timeout_secs
                )
            )
            processes.append(process)
            process.start()

        for process in processes:
            process.join(timeout_secs)
            if process.is_alive():
                logger.error(
                    f"Process for running scripts is still alive after {timeout_secs} seconds. Terminating..."
                )
                process.terminate()
                process.join()

        if has_failures_in_machine_statuses(machine_statuses):
            logger.error(
                f"Errors running scripts on machines."
            )
            for machine_ip, status in machine_statuses.items():
                if status != "complete":
                    logger.error(
                        f"{machine_ip}: {status}"
                    )

            return update_ess_df(
                new_ess_df,
                start_timestamp,
                datetime.datetime.now(),
                test_name,
                ping_count,
                ssh_check_count,
                "failed script execution",
                qos_config,
                new_ess_row['comments'] + " Errors running scripts on machines."
            )


    # End timestamp
    end_timestamp = datetime.datetime.now()

    # 8. Check and download results.
    local_results_dir = os.path.join(experiment_dirpath, test_name)
    with Manager() as manager:
        machine_statuses = manager.dict()

        # Initialise machine statuses with IP and "pending"
        for machine_config in scripts_per_machine:
            machine_statuses[machine_config['ip']] = "pending"

        processes = []
        for machine_config in scripts_per_machine:
            machine_ip = machine_config['ip']

            process = Process(
                target = download_results_from_machine,
                args = (
                    machine_config,
                    machine_statuses,
                    local_results_dir
                )
            )
            processes.append(process)
            process.start()

        for process in processes:
            process.join(60)
            if process.is_alive():
                logger.error(
                    f"Process for downloading results is still alive after 60 seconds. Terminating..."
                )
                process.terminate()
                process.join()


    # 9. Update ESS.
    new_ess_df = update_ess_df(
        new_ess_df,
        start_timestamp,
        end_timestamp,
        test_name,
        ping_count,
        ssh_check_count,
        "success",
        qos_config,
        new_ess_row['comments']
    )

    # 10. Return ESS.
    return new_ess_df
def generate_test_config_from_qos(qos: Optional[Dict] = None) -> Optional[Dict]:
    if qos is None:
        logger.error("No QoS passed.")
        return None

    # TODO: Implement qos validation

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
        logger.warning(
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
        logger.warning(
            f"Couldn't find end index for summary row for {pub_file}."
        )
        return None

    try:
        lat_df = pd.read_csv(pub_file, skiprows=start_index, nrows=end_index-start_index, on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        # console.print(f"EmptyDataError for {pub_file}.", style="bold red")
        logger.warning(
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
        logger.warning(
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
    # TODO: Validate parameters

    summaries_dirpath = os.path.join(dirpath, "summarised_data")
    os.makedirs(summaries_dirpath, exist_ok=True)

    test_dirpaths = [os.path.join(dirpath, _) for _ in os.listdir(dirpath)]
    test_dirpaths = [_ for _ in test_dirpaths if os.path.isdir(_)]
    test_dirpaths = [_ for _ in test_dirpaths if "summarised_data" not in _.lower()]

    if len(test_dirpaths) == 0:
        logger.warning("Found no test folders in {dirpath}")
        return 

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
    # TODO: Validate parameters
    return
    
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

        logger.debug(f"Checking if PCG or not.")
        is_pcg = get_if_pcg(EXPERIMENT)
        if is_pcg is None:
            logger.warning(
                f"Error checking if {EXPERIMENT['experiment_name']} is PCG or not."
            )
            continue 

        if is_pcg:
            logger.debug(f"Is PCG")
            logger.debug(f"Generating combinations from QoS settings.")

            COMBINATIONS = generate_combinations_from_qos(EXPERIMENT['qos_settings'])

            if COMBINATIONS is None:
                logger.warning(
                    f"Error generating combinations for {EXPERIMENT['experiment_name']}"
                )
                continue

            if len(COMBINATIONS) == 0:
                logger.error(
                    f"No combinations generated for {EXPERIMENT['experiment_name']}"
                )
                continue

            ESS_FILEPATH = os.path.join(EXPERIMENT_DIRNAME, 'ess.csv')

            logger.debug(f"Getting ESS dataframe.")
            ess_df = get_ess_df(ESS_FILEPATH)
            if ess_df is None:
                logger.warning(
                    f"Error getting ESS dataframe."
                )
                continue

            ess_df_row_count = len(ess_df.index)
            starting_test_index = ess_df_row_count

            for test_config in COMBINATIONS[starting_test_index:]:
                ess_df = get_ess_df(ESS_FILEPATH)
                if ess_df is None:
                    logger.warning(
                        f"Error getting ESS dataframe."
                    )
                    continue

                test_index = COMBINATIONS.index(test_config)

                test_name = get_test_name_from_combination_dict(test_config)
                if test_name is None:
                    logger.warning(
                        f"Couldn't get the name of the next test to run for {EXPERIMENT['experiment_name']}."
                    )
                    continue

                if len(ess_df.index) > 10:
                    if have_last_n_tests_failed(ess_df, 10):
                        logger.error(
                            f"Last 10 tests have failed. Quitting..."
                        )
                        break

                logger.info(f"[{test_index + 1}/{len(COMBINATIONS)}] Running test {test_name}...")
                ess_df = run_test(
                    test_config, 
                    EXPERIMENT['slave_machines'],
                    ess_df,
                    EXPERIMENT_DIRNAME
                )
                if ess_df is None:
                    logger.error(f"Error when running test #{test_index + 1}: {test_name}")
                    continue

                ess_df.to_csv(ESS_FILEPATH, index = False)

            logger.debug("PCG experiment complete.")

        else:
            logger.debug(f"Is RCG")
            target_test_count = EXPERIMENT['rcg_target_test_count']

            ESS_FILEPATH = os.path.join(EXPERIMENT_DIRNAME, 'ess.csv')
            logger.debug(f"Getting ESS dataframe.")
            ess_df = get_ess_df(ESS_FILEPATH)
            if ess_df is None:
                logger.warning(
                    f"Error getting ESS dataframe."
                )
                continue

            ess_df_row_count = len(ess_df.index)
            if ess_df_row_count == target_test_count:
                logger.info(f"Finished running all {ess_df_row_count} tests for {EXPERIMENT['experiment_name']}")
                continue

            remaining_test_count = target_test_count - ess_df_row_count

            for i in range(remaining_test_count):
                ess_df = get_ess_df(ESS_FILEPATH)
                if len(ess_df.index) > 10:
                    if have_last_n_tests_failed(ess_df, 10):
                        logger.error("Last 10 tests have failed. Quitting....")
                        break

                # Generate new combination configuration
                test_config = generate_test_config_from_qos(EXPERIMENT['qos_settings'])
                if test_config is None:
                    logger.error("Error generating RCG config")
                    continue

                test_name = get_test_name_from_combination_dict(test_config)
                if test_name is None:
                    logger.error("Couldn't get test name for config")
                    continue

                # Run test
                logger.info(f"[{i}/{target_test_count}] Running test {test_name}...")
                ess_df = run_test(
                    test_config,
                    EXPERIMENT['slave_machines'],
                    ess_df,
                    EXPERIMENT_DIRNAME
                )

                if ess_df is None:
                    logger.error(f"Error when running test #{test_index + 1}: {test_name}")
                    continue

                ess_df.to_csv(ESS_FILEPATH, index = False)

            
            logger.debug("RCG experiment complete.")

        # Do a check on all tests to make sure expected number of pub and sub files are the same
        test_dirpaths = [os.path.join(EXPERIMENT_DIRNAME, _) for _ in os.listdir(EXPERIMENT_DIRNAME)]
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
        # Reduce data from tests into a single file per test
        summarise_tests(EXPERIMENT_DIRNAME) 
        if summarise_tests is None:
            logger.error(
                f"Error summarising tests for {EXPERIMENT['experiment_name']}"
            )
            continue

        # TODO: Generate dataset with and without transient truncation
        generate_dataset(EXPERIMENT_DIRNAME, truncation_percent=0)
        generate_dataset(EXPERIMENT_DIRNAME, truncation_percent=10)
        generate_dataset(EXPERIMENT_DIRNAME, truncation_percent=25)
        generate_dataset(EXPERIMENT_DIRNAME, truncation_percent=50)

        # Compress results at end of experiment
        logger.info(f"Compressing {EXPERIMENT_DIRNAME} to {EXPERIMENT_DIRNAME}.zip...")
        if os.path.exists(f"{EXPERIMENT_DIRNAME}.zip"):
            logger.warning(f"A compressed version of the results already exists...")

        else:
            shutil.make_archive(
                EXPERIMENT_DIRNAME,
                'zip',
                EXPERIMENT_DIRNAME
            )

if __name__ == "__main__":
    if pytest.main(["-q", "./pytests", "--exitfirst"]) == 0:
        main(sys.argv)
    else:
        logger.error("Tests failed.")
        sys.exit(1)
