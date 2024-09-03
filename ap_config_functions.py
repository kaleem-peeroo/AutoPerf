import os
import logging
import json

from typing import Dict, List, Optional, Tuple
from rich.console import Console
console = Console()

from constants import *
from ap_config_functions import *
from ap_execution_functions import *
from ap_test_functions import *
from campaign_functions import *
from connection_functions import *
from ess_functions import *
from qos_functions import *
from utility_functions import *

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

    with open(config_path, 'r') as f:
        try:
            config = json.load(f)
        except ValueError:
            return None, f"Error parsing JSON for config file: {config_path}"

    if not isinstance(config, list):
        return None, f"Config file does not contain a list: {config_path}"

    for experiment in config:
        if not isinstance(experiment, dict):
            return None, f"{experiment} is NOT a dictionary."
    
        is_experiment_config_valid, validate_dict_error = validate_dict_using_keys(
            list(experiment.keys()),
            REQUIRED_EXPERIMENT_KEYS
        )
        if validate_dict_error:
            return None, f"Error validating {experiment}."
        if not is_experiment_config_valid:
            return None, f"Config invalid for {experiment['experiment_name']} in {config_path}."

        qos_settings = experiment['qos_settings']
        is_qos_config_valid, validate_dict_error = validate_dict_using_keys(
            list(qos_settings.keys()),
            REQUIRED_QOS_KEYS
        )
        if validate_dict_error:
            return None, f"Error validating {experiment}."
        if not is_qos_config_valid:
            return None, f"Config invalid for {experiment}."

        slave_machine_settings = experiment['slave_machines']
        for machine_setting in slave_machine_settings:
            is_slave_machine_config_valid, validate_dict_error = validate_dict_using_keys(
                list(machine_setting.keys()),
                REQUIRED_SLAVE_MACHINE_KEYS
            )
            if validate_dict_error:
                return None, f"Error validating slave machine {machine_setting['machine_name']} for {experiment['experiment_name']}."
            if not is_slave_machine_config_valid:
                return None, f"Config invalid for slave machine {machine_setting['machine_name']} for {experiment['experiment_name']}."

        noise_gen_settings = experiment['noise_generation']
        if noise_gen_settings != {}:
            is_noise_gen_config_valid, validate_dict_error = validate_dict_using_keys(
                list(noise_gen_settings.keys()),
                REQUIRED_NOISE_GENERATION_KEYS
            )
            if validate_dict_error:
                return None, f"Error validating noise generation for {experiment['experiment_name']}."
            if not is_noise_gen_config_valid:
                return None, f"Config invalid for noise generation for {experiment['experiment_name']}."

    return config, None

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

def get_expected_test_count_from_experiment(experiment_config: Dict = {}) -> Tuple[Optional[int], Optional[str]]:
    if experiment_config == {}:
        return None, "No experiment config passed."

    if experiment_config['rcg_target_test_count'] == 0:
        experiment_combinations, experiment_combinations_error = generate_combinations_from_qos(experiment_config['qos_settings'])
        if experiment_combinations_error:
            return None, experiment_combinations_error
          
        expected_test_count = len(experiment_combinations)
    else:
        expected_test_count = experiment_config['rcg_target_test_count']

    return expected_test_count, None
