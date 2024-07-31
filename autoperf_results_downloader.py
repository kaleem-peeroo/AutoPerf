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
from typing import Dict, List, Optional, Tuple
from pprint import pprint
from multiprocessing import Process, Manager
from rich.progress import track
from rich.table import Table
from rich.console import Console
from rich.markdown import Markdown
from io import StringIO
from constants import *

console = Console()

warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd

DEBUG_MODE = False

# Set up logging
logging.basicConfig(
    level=logging.DEBUG, 
    filename="logs/autoperf_results_downloader.log", 
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
            REQUIRED_MONITOR_MACHINE_KEYS
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
            logger.error(
                f"Error running {command} over SSH on {machine_name}: {stderr}"
            )
            return None

    return stdout

def download_items_from_machine(
    machine: Dict = {},
    item_type: str = "",
    status: Console.status = None
) -> Tuple[Optional[List], Optional[str]]:

    if machine == {}:
        return None, "No machine config passed."

    if item_type not in ["zipped_dirs", "datasets", "summarised_data"]:
        return None, f"Invalid item type: {item_type}. Must be one of ['zipped_dirs', 'datasets', 'summarised_data']."

    if item_type == "zipped_dirs":
        remote_item_dir = f"~/AutoPerf/{DATA_DIR}"
        backup_remote_dir = "~/AutoPerf/data"
        local_item_dir = f"{DATA_DIR}{machine['name']}"

    elif item_type == "datasets":
        remote_item_dir = f"~/AutoPerf/{DATASET_DIR}"
        backup_remote_dir = "~/AutoPerf/datasets"
        local_item_dir = f"{DATASET_DIR}{machine['name']}"

    elif item_type == "summarised_data":
        remote_item_dir = f"~/AutoPerf/{SUMMARISED_DIR}"
        backup_remote_dir = "~/AutoPerf/summarised_data"
        local_item_dir = f"{SUMMARISED_DIR}{machine['name']}"

    else:
        return None, f"Invalid item type: {item_type}. Must be one of ['zipped_dirs', 'datasets', 'summarised_data']."


    status.update(f"Getting {item_type} from {machine['name']} ({machine['ip']})...")

    get_items_command = f"ls {remote_item_dir}"
    get_items_output = run_command_via_ssh(
        machine,
        get_items_command
    )
    if get_items_output is None:
        return None, f"Couldn't get {item_type} from {machine['name']} ({machine['ip']})."

    os.makedirs(local_item_dir, exist_ok=True)

    item_dirs = get_items_output.split()
    if len(item_dirs) == 0:
        console.print(f"No {item_type} found on {machine['name']}.", style="bold yellow")
        status.update(f"Checking {backup_remote_dir} as backup.")
        if item_type == "zipped_dirs":
            get_items_command = f"ls {backup_remote_dir}.zip"
        else:
            get_items_command = f"ls {backup_remote_dir}"

        get_items_output = run_command_via_ssh(
            machine,
            get_items_command
        )
        if get_items_output is None:
            return None, f"Couldn't get {item_type} from {machine['name']} ({machine['ip']})."

        item_dirs = get_items_output.split()
        if len(item_dirs) == 0:
            return None, f"No {item_type} found on {machine['name']} (including old location - {backup_remote_dir})."

        console.print(f"Found {len(item_dirs)} {item_type} in old location ({backup_remote_dir}).", style="bold yellow")
        remote_item_dir = backup_remote_dir

    if item_type == "summarised_data":
        console.print(Markdown(f"## Summarised Data"))
    elif item_type == "datasets":
        console.print(Markdown(f"## Datasets"))
    else:
        console.print(Markdown(f"## Raw Data"))

    for item_dir in item_dirs:
        i = item_dirs.index(item_dir)
        i += 1
        counter_string = f"[{i}/{len(item_dirs)}]"
        item_dir = os.path.basename(item_dir)

        get_file_size_command = f"du -sh {remote_item_dir}/{item_dir}".replace("//", "/")
        get_file_size_output = run_command_via_ssh(
            machine,
            get_file_size_command
        )
        if get_file_size_output is None:
           console.print(f"Couldn't get file size of {item_dir} from {machine['name']}", style="bold red")
           continue

        file_size = get_file_size_output.split()[0]
        status.update(f"{counter_string} Downloading {item_dir} ({file_size}B) from {machine['name']}...")

        if item_type == "summarised_data":
            download_command = f"scp -r"
        else:
            download_command = f"scp -i"

        download_command = f"{download_command} {machine['ssh_key_path']} {machine['username']}@{machine['ip']}:{remote_item_dir}/{item_dir} {local_item_dir}".replace("//", "/")

        download_output = subprocess.Popen(
            download_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        stdout, stderr = download_output.communicate()
        stdout = stdout.decode('utf-8').strip()
        stderr = stderr.decode('utf-8').strip()

        if download_output.returncode != 0:
            console.print(f"{counter_string} ❌ {item_dir} ({file_size}B)\n\t{stderr}", style="bold red")
            continue

        console.print(f"{counter_string} ✅ {item_dir} ({file_size}B)", style="bold green")
        
    return item_dirs, None
   
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
        machine_name = MACHINE_CONFIG['name']
        console.print(Markdown(f"# {machine_name} ({MACHINE_CONFIG['ip']})"))

        with console.status(f"Downloading data from {machine_name}...") as status:
            for item_type in ["zipped_dirs", "datasets", "summarised_data"]:
                _, item_error = download_items_from_machine(MACHINE_CONFIG, item_type, status)
                if item_error:
                    console.print(item_error, style="bold red")

if __name__ == "__main__":
    main(sys.argv)

