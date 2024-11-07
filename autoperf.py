import subprocess
import sys
import time
import ast
import itertools
import re
import os
import logging
import json
import datetime
import warnings
import random
import shutil
import socket
import smtplib
import asyncio
import toml

from tapo import ApiClient
from pprint import pprint
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from my_secrets import APP_PASSWORD, TAPO_USERNAME, TAPO_PASSWORD
from typing import Dict, List, Optional, Tuple
from pprint import pprint
from multiprocessing import Process, Manager
from rich.progress import track
from rich.console import Console
console = Console()

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd

from constants import *

DEBUG_MODE = True
SKIP_RESTART = False

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

async def restart_tapo_plug_from_machine_name(machine_name: str = ""):
    if machine_name == "":
        logger.warning("No machine name given to restart tapo plug.")
        return

    if machine_name.lower().strip() not in ['k2', 'k3', 'k4', 'k5', 'p1', 'p2']:
        logger.warning("Machine name is not p1, p2, k2, k3, k4, or k5")
        return

    TAPO_PLUGS = [
        {"name": "k2", "ip": "192.168.1.102"},
        {"name": "k3", "ip": "192.168.1.103"},
        {"name": "k4", "ip": "192.168.1.104"},
        {"name": "k5", "ip": "192.168.1.105"},
        {"name": "p1", "ip": "192.168.1.106"},
        {"name": "p2", "ip": "192.168.1.107"},
    ]

    chosen_plug = None
    for PLUG in TAPO_PLUGS:
        if PLUG['name'] == machine_name:
            chosen_plug = PLUG
            break
    if chosen_plug == None:
        logger.warning(f"Couldn't get IP for {machine_name}")
        return

    client = ApiClient(TAPO_USERNAME, TAPO_PASSWORD)
    try:
        device = await client.p100(chosen_plug['ip'])
    except Exception as e:
        logger.error(f"Failed to get plug to restart for {chosen_plug}:\n\t{e}")
        with open('output/tapo_restart.log', 'a+') as f:
            date_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
            f.write(
                f"[{date_timestamp}] Failed to restart {chosen_plug['name']} with IP {chosen_plug['ip']}"
            )
        return

    logger.info(f"Turning off {machine_name}")
    await device.off()

    logger.info(f"Waiting 3 seconds")
    await asyncio.sleep(2)

    logger.info(f"Turning on {machine_name}")
    await device.on()

    with open('output/tapo_restart.log', 'a+') as f:
        date_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S')
        f.write(f"[{date_timestamp}] Restarted {chosen_plug['name']} with IP {chosen_plug['ip']}")

def send_email(
    subject: str = "", 
    body: str = ""
) -> Optional[Tuple[bool, Optional[str]]]:
    if body == "":
        return False, "Body is empty"

    if subject == "":
        return False, "Subject is empty"

    sender = 'KaleemPeeroo@gmail.com'
    recipients = ['KaleemPeeroo@gmail.com']
    path_to_file = ''

    message = MIMEMultipart()
    message['From'] = sender
    message['To'] = ', '.join(recipients)
    message['Subject'] = subject
    body_part = MIMEText(body)
    message.attach(body_part)

    # with open(path_to_file, 'r') as file:
    #     attachment = MIMEApplication(file.read(), Name='')
    #     message.attach(attachment)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender, APP_PASSWORD)
        server.sendmail(sender, recipients, message.as_string())

    return True, None

def check_connection(machine, connection_type="ping"):
    # TODO: Validate parameters 
    # connection type can be only ping or ssh

    name = machine['machine_name']
    username = machine['username']
    ip = machine['ip']
    
    if connection_type == "ping":
        command = ["ping", "-c", "5", "-W", "10", ip]
    else:
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

def ping_machine(ip: str = "") -> Tuple[Optional[bool], Optional[str]]:
    """
    Ping a machine to check if it's online.

    Params:
        - ip (str): IP address of the machine to ping.

    Returns:
        - bool: True if machine is online, False if machine is offline.
        - error
    """
    if ip == "":
        return False, f"No IP passed for connection check."

    logger.info(
        f"Pinging {ip}"
    )

    # -W 60 means a timeout of 60 seconds
    response = os.system(f"ping -c 1 -W 60 {ip} > /dev/null 2>&1")

    if DEBUG_MODE:  
        logger.info(f"ping response: {response}")

    if response == 0:
       return True, None
    else:
        return False, f"Failed to ping {ip}"

def check_ssh_connection_with_socket(machine_config: Dict = {}) -> Tuple[Optional[bool], Optional[str]]:
    ssh_key_path = machine_config['ssh_key_path']
    username = machine_config['username']
    ip = machine_config['ip']

    logger.info(f"SSHing into {username}@{ip}")
    
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.connect((ip, 22))

    except Exception as e:
        return False, e
    else:
        return True, None
    
def check_ssh_connection(machine_config: Dict = {}) -> Tuple[Optional[bool], Optional[str]]:
    """
    Check if an SSH connection can be established to a machine.

    Params:
        - machine_config (Dict): Configuration for the machine to connect to.

    Returns:
        - bool: True if SSH connection is successful, False
        - error
    """
    if machine_config == {}:
        return False, f"No machine config passed to check_ssh_connection()."
    
    # ssh_key_path = machine_config['ssh_key_path']
    username = machine_config['username']
    ip = machine_config['ip']

    logger.info(f"SSHing into {username}@{ip}")

    try:
        response = os.system(
            f"ssh -o \"ConnectTimeout 60\" {username}@{ip} exit > /dev/null 2>&1"
        )

        if response != 0:
            return False, f"SSH check failed with exit code {response}"
        else:
            return True, None

    except subprocess.TimeoutExpired:
        return False, "SSH check timed out after 60 seconds"

    except Exception as e:
        return False, f"Exception occured when checking SSH: {e}"

def get_difference_between_lists(
    list_one: List = [], 
    list_two: List = []
) -> Tuple[Optional[List], Optional[str]]:
    """
    Get the difference between two lists.

    Params:
        - list_one (List): First list.
        - list_two (List): Second list.

    Returns:
        - List: List of items that are in list_one but not in list_two.
        - error 
    """
    if list_one is None:
        return None, f"List one is none."

    if list_two is None:
        return None, f"List two is none."

    longer_list = get_longer_list(
        list_one, 
        list_two
    )
    if longer_list is None:
        return None, f"Couldn't get longer list"

    shorter_list = get_shorter_list(
        list_one, 
        list_two
    )
    if shorter_list is None:
        return None, f"Couldn't get shorter list"

    return [item for item in longer_list if item not in shorter_list], None

def get_longer_list(list_one: List = [], list_two: List = []):
    """
    Get the longer of two lists.

    Params:
        - list_one (List): First list.
        - list_two (List): Second list.

    Returns:
        - List: Longer of the two lists.
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
    Get the shorter of two lists.

    Params:
        - list_one (List): First list.
        - list_two (List): Second list.

    Returns:
        - List: Shorter of the two lists.
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

def validate_dict_using_keys(
        given_keys: List = [], 
        required_keys: List = []
) -> Tuple[ Optional[bool], Optional[str] ]:
    """
    Validate a dictionary using a list of required keys.

    Params:
        - given_keys (List): Keys in the dictionary to validate.
        - required_keys (List): Required keys for the dictionary.

    Returns:
        - bool: True if all required keys are present in the given keys, False otherwise.
        - error: if there is an error
    """
    if given_keys == []:
        return False, f"No given_keys given."

    if required_keys == []:
        return False, f"No required_keys given."

    list_difference, difference_error = get_difference_between_lists(
        list(given_keys), 
        required_keys
    )
    if difference_error:
        return False, f"Error comparing keys for {given_keys}"

    if len(list_difference) > 0:
        given_keys_string = "\n\t - ".join(given_keys)
        list_difference_string = "\n\t - ".join(list_difference)
        return False, f"Mismatch in keys for \n\t{given_keys_string}: \n\n{list_difference_string}\n"
    
    return True, None

def read_config(config_path: str = "") -> Tuple[ Optional[Dict], Optional[str] ]:
    """
    Read a JSON config file.

    Params:
        - config_path (str): Path to the JSON config file.

    Returns:
        - List: List of dictionaries if the config file is valid, None otherwise.
        - error: any errors

    """
    if config_path == "":
        return None, f"No config path passed to read_config()"

    if not os.path.exists(config_path):
        return None, f"{config_path} doesn't exist as a path."

    if config_path.endswith(".json"):
        with open(config_path, 'r') as f:
            try:
                config = json.load(f)
            except ValueError as e:
                return None, f"Error parsing JSON for config file: {config_path}: \n\t{e}"

    elif config_path.endswith(".toml"):
        with open(config_path, "r") as f:
            config = toml.load(f)
            config = config['campaigns']

    else:
        return None, f"Couldn't process the config file. Unknown file extension: {config_path}"

    if not isinstance(config, list):
        return None, f"Config file does not contain a list: {config_path}"

    for campaign in config:
        if not isinstance(campaign, dict):
            return None, f"{campaign} is NOT a dictionary."
    
        is_campaign_config_valid, validate_dict_error = validate_dict_using_keys(
            list(campaign.keys()),
            REQUIRED_CAMPAIGN_KEYS
        )
        if validate_dict_error:
            return None, f"Error validating {campaign['campaign_name']}\n\t{validate_dict_error}"
        if not is_campaign_config_valid:
            return None, f"Config invalid for {campaign['campaign_name']} in {config_path}."

        qos_settings = campaign['qos_settings']
        is_qos_config_valid, validate_dict_error = validate_dict_using_keys(
            list(qos_settings.keys()),
            REQUIRED_QOS_KEYS
        )
        if validate_dict_error:
            return None, f"Error validating {campaign['campaign_name']}: {validate_dict_error}."
        if not is_qos_config_valid:
            return None, f"Config invalid for {campaign}."

        slave_machine_settings = campaign['slave_machines']
        for machine_setting in slave_machine_settings:
            is_slave_machine_config_valid, validate_dict_error = validate_dict_using_keys(
                list(machine_setting.keys()),
                REQUIRED_SLAVE_MACHINE_KEYS
            )
            if validate_dict_error:
                return None, f"Error validating slave machine {machine_setting['machine_name']} for {campaign['campaign_name']}."
            if not is_slave_machine_config_valid:
                return None, f"Config invalid for slave machine {machine_setting['machine_name']} for {campaign['campaign_name']}."

        noise_gen_settings = campaign['noise_generation']
        if noise_gen_settings != {}:
            is_noise_gen_config_valid, validate_dict_error = validate_dict_using_keys(
                list(noise_gen_settings.keys()),
                REQUIRED_NOISE_GENERATION_KEYS
            )
            if validate_dict_error:
                return None, f"Error validating noise generation for {campaign['campaign_name']}: {validate_dict_error}"
            if not is_noise_gen_config_valid:
                return None, f"Config invalid for noise generation for {campaign['campaign_name']}."

    return config, None

def get_if_pcg(campaign: Optional[Dict] = None) -> Tuple[ Optional[bool], Optional[str] ]:
    """
    Check if the campaign is PCG or RCG by checking the combination_generation_type.

    Params:
        - campaign (Dict): campaign config.

    Returns:
        - bool: True if PCG, False if RCG, None otherwise.
        - error
    """
    if campaign is None:
        return False, f"No campaign given."

    if 'combination_generation_type' not in campaign.keys():
        return False, f"combination_generation_type option not found in campaign config."

    if campaign['combination_generation_type'] == "":
        return False, "combination_generation_type is empty."

    combination_generation_type = campaign['combination_generation_type']
    if combination_generation_type not in ['pcg', 'rcg']:
        return False, f"Invalid value for combination generation type: {combination_generation_type}.\n\tExpected either PCG or RCG."
    
    return campaign['combination_generation_type'] == 'pcg', None

def get_valid_dirname(dir_name: str = "") -> Tuple[ Optional[str], Optional[str]]:
    """
    Validate a directory name by replacing invalid characters with underscores.

    Params:
        - dir_name (str): Directory name to validate.

    Returns:
        - str: Valid directory name if valid, None otherwise
        - error
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

def get_campaign_dirpath(campaign: Optional[Dict] = None) -> Tuple[ Optional[str], Optional[str] ]:
    """
    Get a valid directory path from the campaign config by appending the campaign name to the data directory.

    Params:
        - campaign (Dict): campaign config.

    Returns:
        - str: Valid dirname if valid, None otherwise.
        - error
    """
    if campaign is None:
        return None, f"No campaign config passed."

    campaign_dirname, dirname_error = get_valid_dirname(campaign['campaign_name'])
    if dirname_error:
        return None, f"Couldn't get a valid dirname for {campaign_name}"

    campaign_dirname = os.path.join(DATA_DIR, campaign_dirname)

    return campaign_dirname, None

def generate_combinations_from_qos(qos: Dict = {}) -> Tuple[Optional[List], Optional[str]]:
    """
    Generate combinations of QoS settings from the given QoS settings.

    Params:
        - qos (Dict): QoS settings.

    Returns:
        - List: List of dictionaries containing all possible combinations of QoS settings.
        - error
    """
    if qos is None:
        return None, f"No QoS passed."

    if qos == {}:
        return None, f"No QoS passed."

    keys = qos.keys()
    if len(keys) == 0:
        return None, f"No options found for qos"

    for key in keys:
        if key not in REQUIRED_QOS_KEYS:
            return None, f"Found an unexpected QoS setting: {key}"

    values = qos.values()
    if len(values) == 0:
        return None, f"No values found for qos"

    for value in values:
        if len(value) == 0:
            return None, f"One of the settings has no values."

    combinations = list(itertools.product(*values))
    combination_dicts = [dict(zip(keys, combination)) for combination in combinations]

    if len(combination_dicts) == 0:
        return None, f"No combinations were generated fro mthe QoS values:\n\t {qos}"

    return combination_dicts, None

def get_ess_df(ess_filepath: str = "") -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """

    Get the ESS dataframe from the given filepath.
    Params:
        - ess_filepath (str): Filepath for the ESS dataframe.

    Returns:
        - pd.DataFrame: ESS dataframe if valid, None otherwise.
        - error
    """
    if ess_filepath == "":
        return None, f"No filepath passes for ESS."

    ess_exists = os.path.exists(ess_filepath)
    if ess_exists:
        try:
            ess_df = pd.read_parquet(ess_filepath, engine='fastparquet')
        except Exception as e:
            return None, f"Couldn't read {ess_filepath}: \n\t{e}"
    else:
        ess_df = pd.DataFrame(columns=[
            "start_timestamp",
            "end_timestamp",
            "test_name",
            "pings_count",
            "ssh_check_count",
            "end_status",
            "qos_settings",
            "scripts_per_machine",
            "comments"
       ])

    return ess_df, None

def get_test_name_from_combination_dict(combination_dict: Dict = {}) -> Tuple[Optional[str], Optional[str]]:
    """
    Get the test name from the combination dict by concatenating the values of the dict together.

    Params:
        - combination_dict (Dict): Combination dict.

    Returns:
        - str: Test name if valid, None otherwise.
        - error
    """
    if combination_dict == {}:
        return None, f"No combination dict passed."

    if combination_dict is None:
        return None, f"Combination dict is None."

    if combination_dict.keys() == []:
        return None, f"No keys found in combination dict."

    diff_between_keys, diff_error = get_difference_between_lists(
        list(combination_dict.keys()),
        REQUIRED_QOS_KEYS
    )
    if diff_error:
        return None, f"Error comparing keys for {combination_dict}"

    if len(diff_between_keys) > 0:
        return None, f"Invalid configuration options in combination dict."

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
            return None, f"Value for {key} is empty."

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

    return test_name, None

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

            qos_dict['use_multicast'] = use_multicast

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

def generate_scripts_from_qos_config(qos_config: Dict = {}) -> Optional[List]:
    """
    Generate executable shell scripts from the given QoS config.

    Params:
        - qos_config (Dict): QoS config.

    Returns:
        - List: List of scripts if valid, None otherwise.
    """
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
    """
    Get machines by type from the given machine configs e.g. pub or sub.

    Params:
        - machine_configs (List): List of machine configs.
        - machine_type (str): Type of machine to get.

    Returns:
        - List: List of machines of the given type.
    """
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
    """
    Distribute scripts to machines based on the machine type.

    Params:
        - scripts (List): List of scripts to distribute.
        - machine_configs (List): List of machine configs.

    Returns:
        - List: List of machine configs with scripts distributed.
    """
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
    """
    Get the buffer duration in seconds from the test duration.
    This is 5% of the test duration with a minimum of 30 seconds.

    Params:
        - test_duration_secs (int): Test duration in seconds.

    Returns:
        - int: Buffer duration in seconds.
    """
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

    if buffer_duration_sec < 60:
        buffer_duration_sec = 60

    return int(buffer_duration_sec)

def has_failures_in_machine_statuses(machine_statuses) -> Optional[bool]:
    """
    Check if there are any failures in the machine statuses.

    Params:
        - machine_statuses (Dict): Machine statuses.

    Returns:
        - bool: True if there are failures, False otherwise.
    """
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
    """
    Update the machine status in the machine statuses.

    Params:
        - machine_statuses (Dict): Machine statuses.
        - machine_ip (str): Machine IP.
        - new_status (str): New status.

    Returns:
        - Dict: Updated machine statuses if valid, None otherwise.
    """
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
        timeout_secs: int = 0,
        test_prefix: str = ""
    ) -> Optional[str]:

    """
    Run a script on a machine using SSH.

    Params:
        - machine_config (Dict): Machine config.
        - machine_statuses (Dict): Machine statuses.
        - timeout_secs (int): Timeout in seconds.
        - test_prefix (str): the campaign and test counter string for messages
            - Looks like:
                "
                [x/y] [camp_name]
                [a/b] [test_name]
                "

    Returns:
        - None
    """
    if machine_config == {}:
        logger.error(
            f"No machine config passed."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no machine config passed"
        )
        return "error: no machine config passed"

    if machine_statuses == {}:
        logger.error(
            f"No machine statuses passed."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no machine statuses passed"
        )
        return "error: no machine statuses passed"

    if timeout_secs == 0:
        logger.error(
            f"No timeout passed to run_script_on_machine()."
        )
        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            "error: no timeout passed"
        )
        return "error: no timeout passed"

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
        return "error: machine statuses have failures"

    logger.info(
        f"{test_prefix}Running script on {machine_config['machine_name']} ({machine_config['ip']})."
    )

    script_string = machine_config['script']

    # If you see '; &' then remove the ; to make it just ' &'
    if ";" in script_string[-10:] and "&" in script_string[-10:]:
        semi_colon = script_string.rfind(";")
        script_string = script_string[:semi_colon] + script_string[semi_colon + 1:]

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

        if DEBUG_MODE:
            logger.debug(
                f"{test_prefix}{username}@{machine_ip}"
            )
            logger.debug(
                f"{test_prefix}STDOUT:\n\t{stdout}"
            )
            logger.debug(
                f"{test_prefix}STDERR:\n\t{stderr}"
            )
            logger.debug(
                f"""
                {test_prefix}
                Script:
                {script_string}
                """
            )

        if return_code != 0:
            logger.error(
                f"{test_prefix}Error running script on {machine_config['machine_name']}."
            )
            logger.error(
                f"{test_prefix}Return code: \t{return_code}"
            )
            if stdout != "":
                logger.error(
                    f"{test_prefix}stdout: \t{stdout}"
                )
            if stderr != "":
                logger.error(
                    f"{test_prefix}stderr: \t{stderr}"
                )

            update_machine_status(
                machine_statuses,
                machine_config['ip'],
                "error: return code not 0"
            )
        else:
            logger.info(
                f"{test_prefix}Script on {machine_config['machine_name']} ran successfully."
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
            f"{test_prefix}Script on {machine_config['machine_name']} timed out."
        )
        if stdout != "":
            logger.error(
                f"{test_prefix}stdout: \t\t{stdout}"
            )

        if stderr != "":
            logger.error(
                f"{test_prefix}stderr: \t\t{stderr}"
            )

        update_machine_status(
            machine_statuses,
            machine_config['ip'],
            f"error: timed out after {timeout_secs} seconds"
        )

    return None

def download_results_from_machine(machine_config, machine_statuses, local_results_dirpath) -> Optional[None]:
    """
    Download results from the machine to the local results directory.

    Params:
        - machine_config (Dict): Machine config.
        - machine_statuses (Dict): Machine statuses.
        - local_results_dirpath (str): Local results directory path.

    Returns:
        - None
    """
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

    logger.info(
        f"\t\tDownloading results from {machine_name} ({machine_ip})."
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
        logger.info(
            # f"\t\t\t{machine_name}: Downloading...\n\t{remote_csv_filepath} \n\tto \n\t{local_csv_filepath}",
            f"\t\t{machine_name}: Downloading {csv_file}..."
        )
        download_command = f"scp {username}@{machine_ip}:{remote_csv_filepath} {local_csv_filepath}"
        download_process = subprocess.Popen(
            download_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = download_process.communicate(timeout=120)
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
    stdout, stderr = delete_all_csv_process.communicate(timeout=120)
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

    logger.info(
        f"\t\tResults downloaded from {machine_name} ({machine_ip})."
    )

def delete_csvs_from_machines(machine_config) -> Optional[None]:
    """
    Delete all CSV files in the Perftest directory from the machine.

    Params:
        - machine_config (Dict): Machine config.

    Returns:
        - None
    """
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

def update_ess_df(
    ess_df: pd.DataFrame = pd.DataFrame(),
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    test_name: str = "",
    ping_count: int = 0,
    ssh_check_count: int = 0,
    end_status: str = "",
    qos_settings: Dict = {},
    scripts_per_machine: Dict = {},
    comments: str = ""
) -> Optional[pd.DataFrame]:
    """
    Update the ESS dataframe with the new test results.

    Params:
        - ess_df (pd.DataFrame): ESS dataframe.
        - start_timestamp (str): Start timestamp.
        - end_timestamp (str): End timestamp.
        - test_name (str): Test name.
        - ping_count (int): Ping count.
        - ssh_check_count (int): SSH check count.
        - end_status (str): End status.
        - qos_settings (Dict): QoS settings.
        - scirpts_per_machine (Dict): Scripts run per machine.
        - comments (str): Comments.

    Returns:
        - pd.DataFrame: Updated ESS dataframe if valid, None otherwise.
    """
    new_ess_row = {}
    new_ess_row['start_timestamp'] = start_timestamp
    new_ess_row['end_timestamp'] = end_timestamp
    new_ess_row['test_name'] = test_name
    new_ess_row['ping_count'] = ping_count
    new_ess_row['ssh_check_count'] = ssh_check_count
    new_ess_row['end_status'] = end_status
    new_ess_row['qos_settings'] = qos_settings
    new_ess_row['scripts_per_machine'] = scripts_per_machine
    new_ess_row['comments'] = comments

    new_ess_row_df = pd.DataFrame([new_ess_row])
    new_ess_df = pd.concat(
        [ess_df, new_ess_row_df],
        axis = 0,
        ignore_index = True
    )
    return new_ess_df

def get_noise_gen_scripts(config: Dict = {}) -> Tuple[Optional[List[str]], Optional[str]]:
    # TODO: Validate parameters
    # TODO: Write unit tests

    if config == {}:
        return None, "Config not passed."

    packet_loss = config['packet_loss']
    packet_duplication = config['packet_duplication']
    packet_corruption = config['packet_corruption']

    delay_value = config['delay']['value']
    delay_variation = config['delay']['variation']
    delay_distribution = config['delay']['distribution']
    delay_correlation = config['delay']['correlation']
    
    bw_rate = config['bandwidth_rate']

    qdisc_str = "sudo tc qdisc add dev eth0"

    netem_script = f"{qdisc_str} root netem loss {packet_loss}"
    netem_script = f"{netem_script} duplicate {packet_duplication}"
    netem_script = f"{netem_script} rate {bw_rate}"
    netem_script = f"{netem_script} corrupt {packet_corruption}"
    netem_script = f"{netem_script} delay {delay_value}"
    netem_script = f"{netem_script} {delay_variation}"
    netem_script = f"{netem_script} {delay_correlation}"

    if int(delay_value.replace("ms", "")) > 0 and int(delay_variation.replace("ms", "")) > 0:
        netem_script = f"{netem_script} distribution {delay_distribution}"

    scripts = []
    scripts.append(f"sudo tc qdisc del dev eth0 root")
    scripts.append(netem_script)

    return scripts, None
    
def get_file_size_from_filepath(filepath: str = "") -> Tuple[Optional[int], Optional[str]]:
    if filepath == "":
        return None, f"No filepath given to get_file_size_from_filepath()"

    if not os.path.exists(filepath):
        return None, f"File path does NOT exist: {filepath}"

    try:
        size = os.path.getsize(filepath)
        return size, None
    except (OSError, ValueError) as e:
        return None, f"Error getting file size of {filepath}: {e}"

def run_test(
    test_config: Dict = {}, 
    machine_configs: List = [],
    ess_df: pd.DataFrame = pd.DataFrame(),
    campaign_dirpath: str = "",
    noise_gen_config: Dict = {},
    test_prefix: str = ""
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Run the test using the given test config, machine configs and ESS dataframe.

    Params:
        - test_config (Dict): Test config.
            - Example:
                {
                    'datalen_bytes': 100,
                    'durability_level': 0,
                    'duration_secs': 60,
                    'latency_count': 100,
                    'pub_count': 1,
                    'sub_count': 1,
                    'use_multicast': True,
                    'use_reliable': True
                }
        - machine_configs (List): List of machine configs.
        - ess_df (pd.DataFrame): ESS dataframe.
        - campaign_dirpath (str): campaign directory path.
        - nosie_gen_config (Dict): Config for noise generation.

    Returns:
        - pd.DataFrame: Updated ESS dataframe if valid, None otherwise.
        - error
    """
    if test_config == {}:
        return None, f"No test config passed."

    if machine_configs == []:
        return None, f"No machine config passed."

    if ess_df is None:
        return None, f"No ESS dataframe passed."

    if not isinstance(ess_df, pd.DataFrame):
        return None, f"ESS dataframe is not a dataframe."

    if not isinstance(test_config, dict):
        return None, f"Next test config is not a dictionary."

    if not isinstance(machine_configs, List):
        return None, f"Machine config is not a list."

    if campaign_dirpath == "":
        return None, f"No campaign dirpath passed."

    if noise_gen_config is None:
        return None, f"No noise generation config passed."

    if not isinstance(noise_gen_config, Dict):
        return None, f"Noise gen config is not a dict."

    if test_prefix == "":
        return ess_df, "No test prefix string passed."

    new_ess_df = ess_df

    CAMPAIGN_NAME = os.path.basename(campaign_dirpath)

    test_name, test_error = get_test_name_from_combination_dict(test_config)
    if test_error:
        return new_ess_df, f"Couldn't get the name of the next test to run."

    """
    1. Check connections to machine (ping + ssh).
    2. Restart machines.
    3. Check connections to machines (ping + ssh).
    4. Get QoS configuration.
    5. Generate test scripts from QoS config.
    6. Allocate scripts per machine.
    7. Delete any artifact csv files.
    8. Generate noise genertion scripts if needed and add to existing scripts. 
    9. Run scripts.
    10. Check for and download results.
    11. Confirm all files are downloaded.
    12. Updated ESS.
    13. Return ESS.
    """

    start_timestamp = pd.Timestamp.now()

    new_ess_row = {}
    new_ess_row['comments'] = ""

    # 1. Check connections to machine (ping + ssh).
    for machine_config in machine_configs:
        machine_ip = machine_config['ip']
        machine_name = machine_config['machine_name']

        ping_attempts = 3
        while ping_attempts > 0:
            logger.info(
                f"{test_prefix} [{4 - ping_attempts}/3] Pinging {machine_name}"
            )
            was_pinged, ping_error = check_connection(machine_config, "ping")    
            if was_pinged:
                break

            ping_attempts -= 1

        if ping_attempts == 0:
            return update_ess_df(
                new_ess_df,
                start_timestamp,
                datetime.datetime.now(),
                test_name,
                0,
                0,
                f"ping_check_fail",
                test_config,
                {},
                new_ess_row['comments'] + f"Failed to even ping {machine_ip} after 3 attempts."
            ), f"failed initial ping check on {machine_ip}"

        ssh_attempts = 3
        while ssh_attempts > 0 and ping_attempts > 0:
            logger.info(
                f"{test_prefix} [{4 - ssh_attempts}/3] SSHing {machine_name}"
            )
            was_sshed, ssh_error = check_connection(machine_config, "ssh")
            if was_sshed:
                break

            ssh_attempts -= 1

        if ssh_attempts == 0:
            return update_ess_df(
                new_ess_df,
                start_timestamp,
                datetime.datetime.now(),
                test_name,
                0,
                0,
                f"ssh_check_fail",
                test_config,
                {},
                new_ess_row['comments'] + f"Failed to even ssh {machine_ip} after 3 attempts."
            ), f"failed initial ssh check on {machine_ip}"

    logger.info(
        f"{test_prefix} Restarting all machines..."
    )

    # 2. Restart machines.
    if not SKIP_RESTART:
        for machine_config in machine_configs:
            logger.info(
                f"{test_prefix} Restarting {machine_config['machine_name']}..."
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
        
        logger.info(
            f"{test_prefix} All machines have restarted. Waiting 15 seconds..."
        )
        
        time.sleep(15)
        
    # 3. Check connections to machines (ping + ssh).
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
            if check_ssh_connection_with_socket(
                machine_config
            ):
                ssh_check_count = attempt
                break
    
            time.sleep(1)
    
    if ping_count == 5 and ssh_check_count == 5:
        logger.warning(f"{test_prefix} Machines failed connection checks.")

        return update_ess_df(
            new_ess_df,
            start_timestamp,
            datetime.datetime.now(),
            test_name,
            ping_count,
            ssh_check_count,
            "connection_check_fail",
            test_config,
            {},
            new_ess_row['comments'] + f"Failed connection check after 5 pings and ssh checks."
        ), "failed connection checks"
        
    else:
        logger.info(
            f"{test_prefix} All machines are available."
        )

    # 4. Get QoS configuration.
    qos_config = test_config

    # 5. Generate test scripts from QoS config.
    scripts = generate_scripts_from_qos_config(
        qos_config
    )

    if scripts is None:
        logger.error(
            f"{test_prefix} Error generating scripts from: \n\t{qos_config}"
        )
        return update_ess_df(
            new_ess_df,
            start_timestamp,
            datetime.datetime.now(),
            test_name,
            ping_count,
            ssh_check_count,
            "script_generation_fail",
            qos_config,
            {},
            new_ess_row['comments'] + " Failed to generate scripts from qos config."
        ), "failed script generation"

    # 6. Allocate scripts per machine.
    scripts_per_machine = distribute_scripts_to_machines(
        scripts,
        machine_configs
    )
    if scripts_per_machine is None:
        logger.error(f"{test_prefix} Error distributing scripts to machines.")
        return update_ess_df(
            new_ess_df,
            start_timestamp,
            datetime.datetime.now(),
            test_name,
            ping_count,
            ssh_check_count,
            "script_distribution_fail",
            qos_config,
            {},
            new_ess_row['comments'] + " Failed to distribute scripts across machines."
        ), "failed script distribution"

    if len(scripts_per_machine) == 0:
        logger.error(f"{test_prefix} No scripts allocated to machines.")
        return update_ess_df(
            new_ess_df,
            start_timestamp,
            datetime.datetime.now(),
            test_name,
            ping_count,
            ssh_check_count,
            "script_distribution_fail",
            qos_config,
            {},
            new_ess_row['comments'] + " No scripts allocated to machines."
        ), "failed script distribution"

    # 7. Delete any artifact csv files.
    logger.info(
        f"{test_prefix} Deleting .csv files before test..."
    )

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

    logger.info(
        f"{test_prefix}.csv files deleted"
    )

    # 8. Generate noise genertion scripts if needed and add to existing scripts. 
    if noise_gen_config != {}:
        noise_gen_scripts, noise_gen_error = get_noise_gen_scripts(noise_gen_config)
        if noise_gen_error:
            return update_ess_df(
                new_ess_df,
                start_timestamp,
                datetime.datetime.now(),
                test_name,
                ping_count,
                ssh_check_count,
                "noise_script_generation_fail",
                qos_config,
                scripts_per_machine,
                new_ess_row['comments'] + " Failed to generate scripts from noise generation config."
            ), f"failed noise generation script generation: {noise_gen_error}"

        if len(noise_gen_scripts) > 0:
            noise_gen_script = ";".join(noise_gen_scripts)
        else:
            noise_gen_script = ""
            
        for script_per_machine in scripts_per_machine:
            script = script_per_machine['script']
            script_per_machine['script'] = f"{noise_gen_script};{script}"

    # 9. Run scripts.
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
                    timeout_secs,
                    test_prefix
                )
            )
            processes.append(process)
            process.start()

        for index, process in enumerate(processes):
            if index == 0:
                process.join(timeout_secs)
            else:
                process.join(buffer_duration_secs)

            if process.is_alive():
                logger.error(
                    f"{test_prefix} Process for running scripts is still alive after {timeout_secs} seconds. Terminating..."
                )
                process.terminate()
                process.join()
                process.close()

        if has_failures_in_machine_statuses(machine_statuses):
            logger.error(
                f"{test_prefix} Errors running scripts on machines."
            )
            for machine_ip, status in machine_statuses.items():
                if status != "complete":
                    logger.error(
                        f"{test_prefix}{machine_ip}: {status}"
                    )

            return update_ess_df(
                new_ess_df,
                start_timestamp,
                datetime.datetime.now(),
                test_name,
                ping_count,
                ssh_check_count,
                "script_execution_fail",
                qos_config,
                scripts_per_machine,
                new_ess_row['comments'] + " Errors running scripts on machines."
            ), "failed script execution"

    # End timestamp
    end_timestamp = pd.Timestamp.now()

    # 10. Check for and download results.
    local_results_dir = os.path.join(campaign_dirpath, test_name)
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
                    f"{test_prefix} Process for downloading results is still alive after 60 seconds. Terminating..."
                )
                process.terminate()
                process.join()

    # 11. Confirm all files are downloaded.
    expected_csv_file_count = get_expected_csv_file_count_from_test_name(test_name)
    actual_csv_file_count = get_csv_file_count_from_dir(local_results_dir)

    file_count_string = f"{actual_csv_file_count}/{expected_csv_file_count}"
    logger.debug(f"{test_prefix}{file_count_string} downloaded files found.")

    if expected_csv_file_count != actual_csv_file_count:
        return update_ess_df(
            new_ess_df,
            start_timestamp,
            end_timestamp,
            test_name,
            ping_count,
            ssh_check_count,
            f"file_count_mismatch",
            qos_config,
            scripts_per_machine,
            new_ess_row['comments'] + f"expected {expected_csv_file_count} files and found {actual_csv_file_count} files instead."
        ), f"expected {expected_csv_file_count} files and found {actual_csv_file_count} files instead."

    result_files = os.listdir(local_results_dir)
    result_files = [os.path.join(local_results_dir, file) for file in result_files]
    result_files = [_ for _ in result_files if _.lower().endswith(".csv")]

    for result_file in result_files:
        # filesize, filesize_error = get_file_size_from_filepath(result_file)
        # if filesize_error:
        #     logger.error(f"{test_prefix} Error getting filesize: {filesize_error}")
        #     continue

        try:
            result_file_df = pd.read_csv(result_file, nrows=10)

            if result_file_df.empty:
                message = f"{result_file} is empty."
                comments = new_ess_row['comments'] + message
            else:
                continue
            
        except Exception as e:
            message = f"Error reading {result_file}: {e}"
            comments = new_ess_row['comments'] + message

        return update_ess_df(
            new_ess_df,
            start_timestamp,
            end_timestamp,
            test_name,
            ping_count,
            ssh_check_count,
            "empty_file_found",
            qos_config,
            scripts_per_machine,
            comments
        ), message

    # 12. Update ESS
    new_ess_df = update_ess_df(
        new_ess_df,
        start_timestamp,
        end_timestamp,
        test_name,
        ping_count,
        ssh_check_count,
        "success",
        qos_config,
        scripts_per_machine,
        new_ess_row['comments']
    )

    # 13. Return ESS
    return new_ess_df, None

def generate_test_config_from_qos(
    qos: Optional[Dict] = None
) -> Tuple[ Optional[Dict], Optional[str] ]:
    """
    Generate a test config from the given qos settings.

    Params:
        - qos (Dict): QoS settings.

    Returns:
        - Dict: Test config if valid, None otherwise.
    """
    if qos is None:
        return None, f"No QoS passed."

    keys = qos.keys()
    if len(keys) == 0:
        return None, f"No options found for qos"

    for key in keys:
        if key not in REQUIRED_QOS_KEYS:
            return None, f"Found an unexpected QoS setting: {key}"

    values = qos.values()
    if len(values) == 0:
        return None, f"No values found for qos"

    for value in values:
        if len(value) == 0:
            return None, f"One of the settings has no values."

    test_config = {}
    for qos_setting, qos_values in qos.items():
        if len(qos_values) == 1:
            test_config[qos_setting] = qos_values[0]
            continue

        if len(qos_values) > 2:
            return None, f"Can't have more than 2 values for RCG qos setting: {qos_setting}"

        all_items_are_ints = all(isinstance(qos_value, int) for qos_value in qos_values)

        if all_items_are_ints:
            lower_bound = min(qos_values)
            upper_bound = max(qos_values)

            random_int_value = random.randint(lower_bound, upper_bound)

            test_config[qos_setting] = random_int_value

        else:
            test_config[qos_setting] = random.choice(qos_values)
        
    return test_config, None

def get_csv_file_count_from_dir(dirpath: str = "") -> Optional[int]:
    # TODO: Implement unit tests for this function
    """
    Get the number of CSV files in the given directory.

    Params:
        - dirpath (str): Directory path.

    Returns:
        - int: Number of CSV files if valid, None otherwise
    """
    if dirpath == "":
        logger.error(
            f"No directory path passed."
        )
        return None

    if not os.path.exists(dirpath):
        logger.error(
            f"Directory path does not exist."
        )
        return None

    if not os.path.isdir(dirpath):
        logger.error(
            f"Directory path is not a directory."
        )
        return None

    files_in_dir = os.listdir(dirpath)
    csv_files_in_dir = [_ for _ in files_in_dir if _.endswith(".csv")]

    return len(csv_files_in_dir)

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

def get_pub_df_from_pub_0_filepath(pub_file: str = "") -> Optional[pd.DataFrame]:
    """
    Get the publisher dataframe from the pub_0 file.

    Params:
        - pub_file (str): Publisher file path.

    Returns:
        - pd.DataFrame: Publisher dataframe if valid, None otherwise
    """
    if pub_file == "":
        logger.error(
            f"No pub file passed."
        )
        return None

    if not os.path.exists(pub_file):
        logger.error(
            f"Pub file does not exist."
        )
        return None

    if not os.path.isfile(pub_file):
        logger.error(
            f"Pub file is not a file."
        )
        return None

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
        # logger.info(f"Couldn't find end index for {pub_file}.", )
        logger.error(
            f"Couldn't find end index for summary row for {pub_file}."
        )
        return None

    try:
        lat_df = pd.read_csv(pub_file, skiprows=start_index, nrows=end_index-start_index, on_bad_lines="skip")
    except pd.errors.EmptyDataError:
        # logger.info(f"EmptyDataError for {pub_file}.", )
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
        # logger.info(f"Couldn't find latency column for {pub_file}.", )
        logger.error(
            f"Couldn't find latency column for {pub_file}."
        )
        return None

    lat_df = lat_df[latency_col]
    lat_df = lat_df.rename("latency_us")
    
    return lat_df

def get_subs_df_from_sub_files(sub_files: List[str] = []) -> Optional[pd.DataFrame]:
    """
    Produce the subscriber dataframe from the sub files.

    Params:
        - sub_files (List): List of subscriber file paths.

    Returns:
        - pd.DataFrame: Subscriber dataframe if valid, None otherwise
    """
    if len(sub_files) == 0:
        logger.error(
            f"No sub files passed."
        )
        return None

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
    """
    Summarise the data from the tests in the given directory to a folder.

    Params:
        - dirpath (str): Directory path.

    Returns:
        - str: Summarised data directory path if valid, None otherwise.
    """
    if dirpath == "":
        logger.error(
            f"No dirpath passed."
        )
        return None

    output_dir_type = "normal"
    if not os.path.exists(dirpath):
        exp_dirname = os.path.basename(dirpath)
        dirpath_first_part = os.path.dirname(dirpath)
        dirpath_5pi = os.path.join(dirpath_first_part, "5pi", exp_dirname)
        dirpath_3pi = os.path.join(dirpath_first_part, "3pi", exp_dirname)

        if os.path.exists(dirpath_5pi):
            dirpath = dirpath_5pi
            output_dir_type = "5pi"
            logger.info(
                f"Using 5pi directory: {dirpath}"
            )

        elif os.path.exists(dirpath_3pi):
            dirpath = dirpath_3pi
            output_dir_type = "3pi"
            logger.info(
                f"Using 3pi directory: {dirpath}"
            )

        else:
            logger.error(
                f"\nDirectory path does not exist: {dirpath}\n" +
                f"5pi directory does not exist: {dirpath_5pi}\n" +
                f"3pi directory does not exist: {dirpath_3pi}\n"
            )
            return None
                                                                                                    
    campaign_name = os.path.basename(dirpath)

    if output_dir_type == "5pi":
        summaries_dirpath = os.path.join(SUMMARISED_DIR, "5pi", campaign_name)
    elif output_dir_type == "3pi":
        summaries_dirpath = os.path.join(SUMMARISED_DIR, "3pi", campaign_name)
    else:
        summaries_dirpath = os.path.join(SUMMARISED_DIR, campaign_name)

    if os.path.exists(summaries_dirpath):
        summaries_dirpath_files = os.listdir(summaries_dirpath)
        if len(summaries_dirpath_files) > 0:
            logger.error(
                f"Summarised data {summaries_dirpath} already exists."
            )
            return None

    os.makedirs(summaries_dirpath, exist_ok=True)
    test_dirpaths = [os.path.join(dirpath, _) for _ in os.listdir(dirpath)]
    test_dirpaths = [_ for _ in test_dirpaths if os.path.isdir(_)]
    test_dirpaths = [_ for _ in test_dirpaths if "summarised_data" not in _.lower()]
    if len(test_dirpaths) == 0:
        logger.warning(f"Found no test folders in {dirpath}")
        return None

    logger.info(
        f"Summarising {len(test_dirpaths)} tests..."
    )

    summarised_test_count = 0
    for test_dirpath in test_dirpaths:
        test_index = test_dirpaths.index(test_dirpath)
        test_name = os.path.basename(test_dirpath)

        expected_csv_file_count = get_expected_csv_file_count_from_test_name(test_name)
        actual_csv_file_count = get_csv_file_count_from_dir(test_dirpath)

        if expected_csv_file_count != actual_csv_file_count:
            logger.info(
                f"Skipping {test_name} because its missing {expected_csv_file_count - actual_csv_file_count} files"
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

        pub_df = pub_df.to_frame().reset_index()

        sub_csv_files = [_ for _ in csv_files if os.path.basename(_).startswith("sub_")]
        subs_df = get_subs_df_from_sub_files(sub_csv_files)

        df_list = [pub_df, subs_df]
        df = pd.concat(df_list, axis=1)

        # Calculate average and total for subs
        sub_cols = [col for col in df.columns if 'sub' in col.lower()]
        sub_cols_without_sub = ["_".join(col.split("_")[2:]) for col in sub_cols]
        sub_metrics = list(set(sub_cols_without_sub))
        for sub_metric in sub_metrics:
            sub_metric_cols = [col for col in sub_cols if sub_metric in col]
            sub_metric_df = df[sub_metric_cols]

            df['avg_' + sub_metric + "_per_sub"] = sub_metric_df.mean(axis=1)
            df['total_' + sub_metric + "_over_subs"] = sub_metric_df.sum(axis=1)

        df.to_csv(
            os.path.join(
                summaries_dirpath,
                f"{test_name}.csv"
            ),
            index=False
        )

        logger.info(
            f"[{test_index + 1}/{len(test_dirpaths)}] Summarised {test_name}.csv"
        )
        summarised_test_count += 1

    logger.info(
        f"Summarised {summarised_test_count}/{len(test_dirpaths)} tests..."
    )

def generate_dataset(dirpath: str = "", truncation_percent: int = 0) -> Optional[str]:
    # TODO: Write unit tests for this function
    """
    Reduce each csv file in summarised_data folder to a single row in one csv file.

    Params:
        - dirpath (str): Directory path.
        - truncation_percent (int): Truncation percent.

    Returns:
        - None
    """
    if dirpath == "":
        logger.error(
            f"No dirpath passed."
        )
        return None

    campaign_name = os.path.basename(dirpath)
    summaries_dirpath = os.path.join(SUMMARISED_DIR, campaign_name)
    if not os.path.exists(summaries_dirpath):
        logger.error(
            f"Summarised dirpath {summaries_dirpath} does NOT exist."
        )
        return None

    test_csvs = [os.path.join(summaries_dirpath, _) for _ in os.listdir(summaries_dirpath)]
    test_csvs = [_ for _ in test_csvs if _.endswith(".csv")]
    if len(test_csvs) == 0:
        logger.error(
            f"No csv files found in {summaries_dirpath}."
        )
    
    logger.info(
        f"[{campaign_name}] Generating dataset from {len(test_csvs)} tests with {truncation_percent}% truncation..."
    )

    campaign_name = os.path.basename(dirpath)
    current_timestamp = datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"{current_timestamp}_{campaign_name}_dataset_{truncation_percent}_percent_truncation.csv"
    filename = os.path.join("output/datasets", filename)

    dataset_df = pd.DataFrame()
    for test_csv in track(test_csvs, description="Processing..."):
        new_dataset_row = {}

        try:
            test_df = pd.read_csv(test_csv)
        except UnicodeDecodeError:
            logger.error(
                f"UnicodeDecodeError reading {test_csv}."
            )
            continue

        test_name = os.path.basename(test_csv)

        new_dataset_row = get_qos_dict_from_test_name(test_name)
        if new_dataset_row is None:
            logger.error(
                f"Couldn't get qos dict for {test_name}."
            )
            continue

        for key, value in new_dataset_row.items():
            if value == True:
                new_dataset_row[key] = 1
            elif value == False:
                new_dataset_row[key] = 0

        for column in test_df.columns:
            if "index" in column.lower():
                continue

            column_values = test_df[column]
            value_count = len(column_values)

            values_to_truncate = int(
                value_count * (truncation_percent / 100)
            )

            column_values = test_df[column].iloc[values_to_truncate:]
            column_df = pd.DataFrame(column_values)
            
            for PERCENTILE in PERCENTILES:
                new_dataset_row[f"{column}_{PERCENTILE}%"] = column_df.quantile(PERCENTILE / 100).values[0]

            for STAT in DISTRIBUTION_STATS:
                if STAT == "mean":
                    new_dataset_row[f"{column}_mean"] = column_df.mean().values[0]

                elif STAT == "std":
                    new_dataset_row[f"{column}_std"] = column_df.std().values[0]

                elif STAT == "min":
                    new_dataset_row[f"{column}_min"] = column_df.min().values[0]

                elif STAT == "max":
                    new_dataset_row[f"{column}_max"] = column_df.max().values[0]

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
        f"[{campaign_name}] Generated dataset: {filename}"
    )
    return filename

def get_ess_df_from_campaign(campaign_config: Dict = {}) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    if campaign_config == {}:
        return None, "No campaign config passed to get_ess_df_from_campaign()"

    CAMPAIGN_DIRPATH, dirname_error = get_campaign_dirpath(campaign_config)
    if dirname_error:
        return None, dirname_error

    CAMPAIGN_DIRNAME = os.path.basename(CAMPAIGN_DIRPATH)
    ESS_PATH = os.path.join(ESS_DIR, f"{CAMPAIGN_DIRNAME}.parquet")

    ess_df, ess_error = get_ess_df(ESS_PATH)
    if ess_error:
        return None, ess_error

    return ess_df, None

def get_test_gen_type(campaign_config):
    # TODO: Validate parameters
    custom_test_list = campaign_config['custom_test_list']
    comb_gen_type = campaign_config['combination_generation_type']
    
    if len(custom_test_list) > 0:
        return "custom_test_list", None
    elif comb_gen_type.lower().strip() == "pcg":
        return "pcg", None
    elif comb_gen_type.lower().strip() == "rcg":
        return "rcg", None
    else:
        return None, "Couldn't get test generation type."

def get_expected_test_count_from_campaign(campaign_config: Dict = {}) -> Tuple[Optional[int], Optional[str]]:
    if campaign_config == {}:
        return None, "No campaign config passed."

    test_gen_type, error = get_test_gen_type(campaign_config)
    if error:
        return None, error

    if test_gen_type == "pcg":
        campaign_combinations, error = generate_combinations_from_qos(
            campaign_config['qos_settings']
        )
        if error:
            return None, error
          
        expected_test_count = len(campaign_combinations)

    elif test_gen_type == "rcg":
        expected_test_count = campaign_config['rcg_target_test_count']

    elif test_gen_type == "custom_test_list":
        expected_test_count = len(
            campaign_config['custom_test_list']
        )

    return expected_test_count, None

def check_if_ess_rows_match_expected_test_count(campaign_config: Dict = {}) -> Tuple[Optional[bool], Optional[str]]:
    if campaign_config == {}:
        return None, "No campaign config passed."

    ess_df, ess_df_error = get_ess_df_from_campaign(campaign_config)
    if ess_df_error:
        return None, ess_df_error

    ess_row_count = len(ess_df.index)

    expected_test_count, error = get_expected_test_count_from_campaign(campaign_config)
    if error:
        return None, error

    if ess_row_count < expected_test_count:
        return False, None

    return True, None

def get_must_wait_for_self_reboot(ess_df: pd.DataFrame = pd.DataFrame()) -> Tuple[Optional[bool], Optional[str]]:
    if len(ess_df.index) == 0:
        return False, "ESS is empty."

    """
    1. Have last n tests failed?
    2. Yes? Have last n tests failed because of same machine (i.e. all comments share same IP)
    3. Yes? Return True
    """

    have_last_n_test_failed_bool, have_last_n_error = have_last_n_tests_failed(ess_df, 3)
    if have_last_n_error:
        return False, have_last_n_error

    if not have_last_n_test_failed_bool:
        return False, None

    ess_df['ip'] = ess_df['comments'].apply(extract_ip)
    ess_df_tail = ess_df.tail(3)
    all_ips_match = ess_df_tail['ip'].nunique() == 1

    return all_ips_match, None

def extract_ip(comment: str = ""):
    if comment == "":
        # logger.warning("No comment passed to extract_ip()")
        return None

    if 'nan' in str(comment).lower():
        return None

    try:
        ips = re.findall(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', comment)
        return ips[0] if ips else None
    except Exception as e:
        logger.error(f"Error when extracting ip from '{comment}': {e}")
        return None
    
def get_ip_output_from_ess_df(
    ess_df, 
    line_break_point: int = 5
) -> Tuple[str, Dict, Optional[str]]:
    if ess_df is None:
        return "", {}, None

    if 'ip' not in ess_df.columns:
        return "", {}, None

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

    return ip_output, ip_emoji_dict, None

def get_unreachable_machine_ip_from_ess_df(
    ess_df: pd.DataFrame = pd.DataFrame()
) -> Tuple[Optional[str], Optional[str]]:
    if len(ess_df.index) == 0:
        return None, "ESS is empty."

    ess_df['ip'] = ess_df['comments'].apply(extract_ip)
    ess_df['end_status'] = ess_df['end_status'].apply(lambda x: x.lower())

    unreachable_machines = ess_df[ess_df['end_status'].str.contains("fail")]['ip']
    unreachable_machines = unreachable_machines.dropna()

    if len(unreachable_machines) == 0:
        return None, "No unreachable machines found."

    unreachable_machines = unreachable_machines.unique()

    return unreachable_machines[-1], None

def get_machine_name_from_ip(
    machine_name: str = "",
    slave_machines: List[Dict] = []
) -> Tuple[Optional[str], Optional[str]]:
    if machine_name == "":
        return None, "No machine name passed."

    if 'nan' in str(machine_name).lower():
        return None, "Machine name is nan."

    if len(slave_machines) == 0:
        return None, "No slave machines passed."

    for machine in slave_machines:
        if machine['ip'] == machine_name:
            return machine['machine_name'], None

    return None, "Machine name not found."

def get_failed_test_names(ess_df: pd.DataFrame):
    # TODO: Validate parameters
    if ess_df is None:
        return None, "No ess df passed."

    failed_rows = ess_df[ess_df['end_status'] != 'success']

    return failed_rows['test_name'].to_list(), None

def get_custom_test_list(exp_conf) -> Tuple[Optional[list[str]], Optional[str]]:
    return exp_conf['custom_test_list'], None

def make_output_dirs():
    os.makedirs("output", exist_ok=True)
    os.makedirs(ESS_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(SUMMARISED_DIR, exist_ok=True)
    os.makedirs(DATASET_DIR, exist_ok=True)

def check_for_self_reboot(
    ess_df: pd.DataFrame = pd.DataFrame(), 
    campaign_conf: Dict = {}
) -> Optional[str]:

    must_wait_for_self_reboot, error = get_must_wait_for_self_reboot(
        ess_df
    )
    if error:
        logger.warning(f"Couldn't check if self-reboot needed: {error}")

    if must_wait_for_self_reboot:
        machine_ip, error = get_unreachable_machine_ip_from_ess_df(ess_df)
        if error:
            logger.warning(f"Error getting unreachable machine IP: {error}")
            logger.info("Restarting all plugs instead...")
            for machine in campaign_conf['slave_machines']:
                machine_name = machine['machine_name']
                asyncio.run(restart_tapo_plug_from_machine_name(machine_name))

        machine_name, error = get_machine_name_from_ip(
            machine_ip,
            campaign_conf['slave_machines']
        )
        if error:
            logger.error(f"Error getting machine IP: {error}")

            logger.info("Restarting all plugs instead...")
            for machine in campaign_conf['slave_machines']:
                machine_name = machine['machine_name']
                asyncio.run(restart_tapo_plug_from_machine_name(machine_name))

        logger.info(f"Restarting {machine_name}...")
        asyncio.run(restart_tapo_plug_from_machine_name(machine_name))

    return None

def main(sys_args: list[str] = []) -> Optional[None]:
    if len(sys_args) < 2:
        logger.error(
            f"Config filepath not specified."
        )
        return 

    make_output_dirs()
    
    CONFIG_PATH = sys_args[1]

    logger.info(f"Reading AP config: {CONFIG_PATH}", )

    CONFIG, error = read_config(CONFIG_PATH)
    if error:
        logger.error(f"Error reading config: {error}")
        return

    logger.info(f"Found {len(CONFIG)} campaigns.")

    for CAMPAIGN_INDEX, CAMPAIGN_CONF in enumerate(CONFIG):
        CAMPAIGN_NAME = CAMPAIGN_CONF['campaign_name']
        campaign_count_string = f"[{CAMPAIGN_INDEX + 1}/{len(CONFIG)}]"
        campaign_name_string = f"[{CAMPAIGN_NAME}]"
        campaign_prefix = f"\n\t{campaign_count_string} {campaign_name_string}\n\t"

        logger.info(
            f"{campaign_prefix} Starting..."
        )

        CAMPAIGN_DIRPATH, error = get_campaign_dirpath(CAMPAIGN_CONF)
        if error:
            logger.error(f"Error getting campaign dirpath: {error}")
            continue

        os.makedirs(CAMPAIGN_DIRPATH, exist_ok=True)
        logger.info(
            f"{campaign_prefix} Created {CAMPAIGN_DIRPATH}"
        )

        custom_test_list, error = get_custom_test_list(CAMPAIGN_CONF)
        if error:
            logger.error(
                f"{campaign_prefix} Couldn't get custom test list for {CAMPAIGN_NAME}: {error}"
            )
            continue

        TEST_GEN_TYPE, error = get_test_gen_type(CAMPAIGN_CONF)
        if error:
            logger.error(
                f"{campaign_prefix} Error getting test gen type: {error}"
            )
            continue 

        CAMPAIGN_DIRNAME = os.path.basename(CAMPAIGN_DIRPATH)
        ESS_PATH = os.path.join(ESS_DIR, f"{CAMPAIGN_DIRNAME}.parquet")
        ess_df, ess_error = get_ess_df(ESS_PATH)
        if ess_error:
            logger.error(f"{campaign_prefix} Error getting ess: {ess_error}")
            continue

        if TEST_GEN_TYPE == "pcg":
            COMBINATIONS, error = generate_combinations_from_qos(
                CAMPAIGN_CONF['qos_settings']
            )
            if error:
                logger.error(f"{campaign_prefix} Error generating combinations: {error}")
                continue

            target_test_count = len(COMBINATIONS)

        elif TEST_GEN_TYPE == "rcg":
            target_test_count = CAMPAIGN_CONF['rcg_target_test_count']

        elif TEST_GEN_TYPE == "custom_test_list":
            target_test_count = len(custom_test_list)
            
        else:
            loger.error(f"Unknown test gen type: {TEST_GEN_TYPE}")
            continue

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

            error = check_for_self_reboot(
                ess_df,
                CAMPAIGN_CONF
            )
            if error:
                logger.error(f"{campaign_prefix} Error checking for self reboot: {error}")
                continue
            
            if TEST_GEN_TYPE == "pcg":
                test_config = COMBINATIONS[test_index]

            elif TEST_GEN_TYPE == "rcg":
                test_config, error = generate_test_config_from_qos(CAMPAIGN_CONF['qos_settings'])
                if error:
                    logger.error(f"{campaign_prefix} Error generating RCG config: {error}")
                    continue

            elif TEST_GEN_TYPE == "custom_test_list":
                test_config = get_qos_dict_from_test_name(custom_test_list[test_index])

            else:
                logger.error(f"{campaign_prefix} Unknown test gen type: {TEST_GEN_TYPE}")
                continue

            test_name, error = get_test_name_from_combination_dict(
                test_config
            )
            if error:
                logger.error(f"{campaign_prefix} Error getting test name: {error}")
                continue

            test_prefix = f"{counter_string} [{test_name}]\n\t"
            test_prefix = f"{campaign_prefix}{test_prefix}"

            logger.info(
                f"{test_prefix} Running test..."
            )

            ess_df, error = run_test(
                test_config,
                CAMPAIGN_CONF['slave_machines'],
                ess_df,
                CAMPAIGN_DIRPATH,
                CAMPAIGN_CONF['noise_generation'],
                test_prefix
            )

            ess_df.to_parquet(ESS_PATH, index = False)

            if error:
                logger.error(f"{test_prefix} Error running test {test_name}: {error}")
                continue
        
        do_ess_rows_match_test_count, error = check_if_ess_rows_match_expected_test_count(
            CAMPAIGN_CONF
        )
        if error:
            logger.error(f"""
                {error}
                Error checking if ESS row count matches expected test count for {CAMPAIGN_NAME}.
                Skipping post test data processing...""")
            continue

        if not do_ess_rows_match_test_count:
            ess_row_count = len(ess_df.index)
            expected_row_count, error = get_expected_test_count_from_campaign(CAMPAIGN_CONF)
            if error:
                logger.error(f"Error getting expected test count: {error}")
                continue

            logger.warning(f"""
                ESS rows don't match expected test count for {CAMPAIGN_NAME}. 
                ESS Count:  {ess_row_count}
                Expected:   {expected_row_count}
                Skipping post test data processing...""")
            continue

        logger.info(f"{campaign_prefix} Re attempting failed tests")
        logger.debug(f"{campaign_prefix} Getting failed test names")
        failed_test_names, error = get_failed_test_names(ess_df)
        if error:
            logger.error(f"Failed to get which tests failed from ESS DF: {error}")
            continue

        logger.debug(f"{campaign_prefix} Found {len(failed_test_names)} failed tests.")
        logger.debug(f"{campaign_prefix} Getting configs for failed test names")

        failed_test_configs = []
        for test_name in failed_test_names:
            test_config = get_qos_dict_from_test_name(test_name)
            failed_test_configs.append(test_config)

        logger.debug(f"{campaign_prefix} Running failed tests")
        for failed_index, failed_test in enumerate(failed_test_configs):
            counter_str = "[{}/{}]".format(
                failed_index,
                len(failed_test_configs)
            )

            fail_prefix = f"[FAILED RERUNS]"
            test_name = failed_test_names[failed_index]
            test_prefix = "{} [{}]".format(
                counter_str,
                test_name
            )

            test_prefix = "{}{}\n{}".format(
                campaign_prefix,
                fail_prefix,
                test_prefix
            )

            retry_counter = 3
            test_status = "failed"

            while retry_counter > 0 and test_status != "success":
                ess_df, run_test_error = run_test(
                    failed_test,
                    CAMPAIGN_CONF['slave_machines'],
                    ess_df,
                    CAMPAIGN_DIRPATH,
                    CAMPAIGN_CONF['noise_generation'],
                    test_prefix
                )

                if run_test_error:
                    logger.error(f"{campaign_prefix}Error running test {test_name}: {run_test_error}")
                    logger.info(
                        f"[{3 - retry_counter}/3] failed."
                    )
                    retry_counter -= 1
                    continue

                test_status = "success"
                retry_counter -= 1

                ess_df.to_parquet(ESS_PATH, index = False)
        
        # Compress results at end of campaign
        if os.path.exists(f"{CAMPAIGN_DIRPATH}.zip"):
            logger.warning(f"A compressed version of the results already exists...")

        else:
            logger.info(f"Compressing {CAMPAIGN_DIRPATH} to {CAMPAIGN_DIRPATH}.zip...")
            shutil.make_archive(
                CAMPAIGN_DIRPATH,
                'zip',
                CAMPAIGN_DIRPATH
            )

if __name__ == "__main__":
    main(sys.argv)
    
