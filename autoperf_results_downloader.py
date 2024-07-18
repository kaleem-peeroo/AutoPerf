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
from rich.markdown import Markdown
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
   
def download_zipped_dirs_from_machine(machine: Dict = {}, status: Console.status = None) -> Optional[List]:
    if machine == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

    status.update(f"Getting zipped dirs from {machine['name']} ({machine['ip']})...")
    data_dir = "~/AutoPerf/data"
    get_zips_command = f"ls {data_dir}/*.zip"
    get_zips_output = run_command_via_ssh(
        machine,
        get_zips_command
    )
    if get_zips_output is None:
        logger.error(
            f"Couldn't get zipped dirs from {machine['name']} ({machine['ip']})."
        )
        return None

    os.makedirs(f"data/{machine['name']}", exist_ok=True)

    zipped_dirs = get_zips_output.split()

    for zipped_dir in zipped_dirs:
        i = zipped_dirs.index(zipped_dir)
        i += 1
        counter_string = f"[{i}/{len(zipped_dirs)}]"
        zipped_dir = os.path.basename(zipped_dir)

        get_file_size_command = f"du -sh {data_dir}/{zipped_dir}"
        get_file_size_output = run_command_via_ssh(
            machine,
            get_file_size_command
        )
        if get_file_size_output is None:
            logger.error(
                f"Couldn't get file size of {zipped_dir} from {machine['name']}: {stderr}"
            )
            return None

        file_size = get_file_size_output.split()[0]
        status.update(f"{counter_string} Downloading {zipped_dir} ({file_size}B) from {machine['name']}...")

        download_command = f"scp -i {machine['ssh_key_path']} {machine['username']}@{machine['ip']}:{data_dir}/{zipped_dir} ./data/{machine['name']}"
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
            logger.error(
                f"Error downloading {zipped_dir} from {machine['name']}: {stderr}"
            )
            return None

        console.print(f"{counter_string} Downloaded {zipped_dir} ({file_size}B) from {machine['name']}.", style="bold green")
        
    return zipped_dirs

def download_datasets_from_machine(machine: Dict = {}, status: Console.status = None):
    if machine == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

    status.update(f"Getting datasets from {machine['name']} ({machine['ip']})...")

    datasets_dir = "~/AutoPerf/datasets"
    get_datasets_command = f"ls {datasets_dir}"
    get_datasets_output = run_command_via_ssh(
        machine,
        get_datasets_command
    )
    if get_datasets_output is None:
        logger.error(
            f"Couldn't get datasets from {machine['name']} ({machine['ip']})."
        )
        return None

    os.makedirs(f"datasets/{machine['name']}", exist_ok=True)

    datasets = get_datasets_output.split()

    for dataset in datasets:
        i = datasets.index(dataset)
        i += 1
        counter_string = f"[{i}/{len(datasets)}]"

        get_file_size_command = f"du -sh {datasets_dir}/{dataset}"
        get_file_size_output = run_command_via_ssh(
            machine,
            get_file_size_command
        )
        if get_file_size_output is None:
            logger.error(
                f"Couldn't get file size of {dataset} from {machine['name']}"
            )
            return None

        file_size = get_file_size_output.split()[0]
        status.update(f"{counter_string} Downloading {dataset} ({file_size}B) from {machine['name']}...")

        download_command = f"scp -i {machine['ssh_key_path']} {machine['username']}@{machine['ip']}:{datasets_dir}/{dataset} ./datasets/{machine['name']}"
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
            logger.error(
                f"Error downloading {dataset} from {machine['name']}: {stderr}"
            )
            return None

        console.print(f"{counter_string} Downloaded {dataset} ({file_size}B) from {machine['name']}.", style="bold green")
    
def download_summarised_data_dirs_from_machine(machine: Dict = {}, status: Console.status = None):
    if machine == {}:
        logger.error(
            f"No machine config passed."
        )
        return None

    status.update(f"Getting summarised_data from {machine['name']} ({machine['ip']})...")

    summ_dir = "~/AutoPerf/summarised_data"
    get_summaries_command = f"ls {summ_dir}"
    get_summaries_output = run_command_via_ssh(
        machine,
        get_summaries_command
    )
    if get_summaries_output is None:
        logger.error(
            f"Couldn't get summarised data from {machine['name']} ({machine['ip']})."
        )
        return None

    os.makedirs(f"summarised_data/{machine['name']}", exist_ok=True)

    summaries = get_summaries_output.split()

    for summary in summaries:
        i = summaries.index(summary)
        i += 1
        counter_string = f"[{i}/{len(summaries)}]"

        get_file_size_command = f"du -sh {summ_dir}/{summary}"
        get_file_size_output = run_command_via_ssh(
            machine,
            get_file_size_command
        )
        if get_file_size_output is None:
            logger.error(
                f"Couldn't get file size of {summary} from {machine['name']}"
            )
            return None

        file_size = get_file_size_output.split()[0]
        status.update(f"{counter_string} Downloading {summary} ({file_size}B) from {machine['name']}...")

        download_command = f"scp -r -i {machine['ssh_key_path']} {machine['username']}@{machine['ip']}:{summ_dir}/{summary} ./summarised_data/{machine['name']}"
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
            logger.error(
                f"Error downloading {summary} from {machine['name']}: {stderr}"
            )
            return None

        console.print(f"{counter_string} Downloaded {summary} ({file_size}B) from {machine['name']}.", style="bold green")

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
            console.print(Markdown(f"## Raw Data"))
            download_zipped_dirs_from_machine(MACHINE_CONFIG, status)

            console.print(Markdown(f"## Datasets"))
            download_datasets_from_machine(MACHINE_CONFIG, status)

            console.print(Markdown(f"## Summarised Data"))
            download_summarised_data_dirs_from_machine(MACHINE_CONFIG, status)
    
if __name__ == "__main__":
    if pytest.main(["-q", "./pytests", "--exitfirst"]) == 0:
        main(sys.argv)
    else:
        logger.error("Tests failed.")
        sys.exit(1)
