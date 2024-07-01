import pandas as pd
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
from icecream import ic
from typing import Dict, List, Optional
from pprint import pprint

DEBUG_MODE = True

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

def check_ssh_connection(ssh_key_path: str = "", username: str = "", ip: str = "") -> Optional[bool]:
    if ssh_key_path == "":
        logger.error(
            f"No SSH key path passed for connection check."
        )
        return None

    if username == "":
        logger.error(
            f"No username passed for connection check."
        )
        return None

    if ip == "":
        logger.error(
            f"No IP passed for connection check."
        )
        return None

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
            "attempt_number",
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
        logger.error(
            f"ESS dataframe has less than {n} tests."
        )
        return None

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

                    
def run_test(
    next_test_config: Dict = {}, 
    machine_configs: List = [],
    ess_df: pd.DataFrame = pd.DataFrame()
) -> Optional[pd.DataFrame]:
    if next_test_config == {}:
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

    if not isinstance(next_test_config, dict):
        logger.error(
            f"Next test config is not a dictionary."
        )
        return None

    if not isinstance(machine_configs, List):
        logger.error(
            f"Machine config is not a list."
        )
        return None

    new_ess_df = ess_df

    next_test_name = get_test_name_from_combination_dict(next_test_config)
    if next_test_name is None:
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

    if not DEBUG_MODE:
        # 1. Check connections to machines.
        for machine_config in machine_configs:
            machine_ip = machine_config['ip']
            if not ping_machine(machine_ip):
                logger.error(
                    f"Couldn't ping {machine_ip}."
                )
                return None
        
            if not check_ssh_connection(
                machine_config['ssh_key_path'],
                machine_config['username'],
                machine_ip
            ):
                logger.error(
                    f"Couldn't SSH into {machine_ip}."
                )
                return None
        
        logger.debug(
            f"Restarting all machines"
        )
        
        # 2. Restart machines.
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
        for machine_config in machine_configs:
            machine_ip = machine_config['ip']
            machine_name = machine_config['machine_name']
           
            # Ping machine up to 5 times.
            for attempt in range(1, 6):
                if ping_machine(machine_ip):
                    new_ess_row['ping_count'] = attempt
                    break
               
                if attempt == 5:
                    logger.error(
                        f"Couldn't ping {machine_name} ({machine_ip}) after 5 attempts after restart."
                    )
                    new_ess_row['comments'] = new_ess_row['comments'] + f"Couldn't ping {machine_name} ({machine_ip}) after 5 attempts after restart. "
                    break
        
                time.sleep(3)
        
            # SSH into machine up to 5 times.
            for attempt in range(1, 6):
                if check_ssh_connection(
                    machine_config['ssh_key_path'],
                    machine_config['username'],
                    machine_ip
                ):
                    new_ess_row['ssh_check_count'] = attempt
                    break
        
                if attempt == 5:
                    logger.error(
                        f"Couldn't SSH into {machine_name} ({machine_ip}) after 5 attempts after restart."
                    )
                    new_ess_row['comments'] = new_ess_row['comments'] + f"Couldn't SSH into {machine_name} ({machine_ip}) after 5 attempts after restart. "
                    break
        
                time.sleep(1)
        
        logger.debug(
            f"All machines are up and running."
        )

    # 4. Get qos config.
    qos_config = next_test_config

    # 5. Generate scripts.
    scripts = generate_scripts_from_qos_config(
        qos_config
    )

    if scripts is None:
        logger.error(
            f"Error generating scripts from: \n\t{qos_config}"
        )
        return None

    # 6. Allocate scripts to machines.
    scripts_per_machine = distribute_scripts_to_machines(
        scripts,
        machine_configs
    )

    # 7. Run scripts.
    pprint(scripts_per_machine)

    # 8. Check results.

    # 9. Update ESS.

    # 10. Return ESS.
    return new_ess_df

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

            logger.debug(f"Getting ESS dataframe.")
            ESS_FILEPATH = os.path.join(EXPERIMENT_DIRNAME, 'ess.csv')
            ess_df = get_ess_df(ESS_FILEPATH)
            if ess_df is None:
                logger.warning(
                    f"Error getting ESS dataframe."
                )
                continue

            logger.debug(f"Getting the next test to run.")
            next_test_name = get_test_name_from_combination_dict(COMBINATIONS[0])
            if next_test_name is None:
                logger.warning(
                    f"Couldn't get the name of the next test to run for {EXPERIMENT['experiment_name']}."
                )
                continue

            next_test_config = COMBINATIONS[0]
            if len(ess_df.index) > 0:
                if have_last_n_tests_failed(ess_df, 10):
                    logger.error(
                        f"Last 10 tests have failed. Quitting..."
                    )
                    return None

                next_test_config = get_next_test_from_ess(ess_df)
                if next_test_config is None:
                    next_test_config = COMBINATIONS[0]

                if next_test_config == {}:
                    next_test_config = COMBINATIONS[0]

            next_test_index = COMBINATIONS.index(next_test_config)

            logger.debug(f"[{next_test_index + 1}/{len(COMBINATIONS)}] Running test {next_test_name}...")
            run_test(
                next_test_config, 
                EXPERIMENT['slave_machines'],
                ess_df
            )

        else:
            logger.debug(f"Is RCG")

if __name__ == "__main__":
    if pytest.main(["-q", "./pytests", "--exitfirst"]) == 0:
        main(sys.argv)
    else:
        logger.error("Tests failed.")
        sys.exit(1)
