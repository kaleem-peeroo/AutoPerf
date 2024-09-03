import os
import logging

from typing import Dict, Optional, Tuple
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

def get_if_pcg(experiment: Optional[Dict] = None) -> Tuple[ Optional[bool], Optional[str] ]:
    """
    Check if the experiment is PCG or RCG by checking the combination_generation_type.

    Params:
        - experiment (Dict): Experiment config.

    Returns:
        - bool: True if PCG, False if RCG, None otherwise.
        - error
    """
    if experiment is None:
        return False, f"No experiment given."

    if 'combination_generation_type' not in experiment.keys():
        return False, f"combination_generation_type option not found in experiment config."

    if experiment['combination_generation_type'] == "":
        return False, "combination_generation_type is empty."

    combination_generation_type = experiment['combination_generation_type']
    if combination_generation_type not in ['pcg', 'rcg']:
        return False, f"Invalid value for combination generation type: {combination_generation_type}.\n\tExpected either PCG or RCG."
    
    return experiment['combination_generation_type'] == 'pcg', None

def get_dirname_from_experiment(experiment: Optional[Dict] = None) -> Tuple[ Optional[str], Optional[str] ]:
    """
    Get a valid dirname from the experiment config by appending the experiment name to the data directory.

    Params:
        - experiment (Dict): Experiment config.

    Returns:
        - str: Valid dirname if valid, None otherwise.
        - error
    """
    if experiment is None:
        return None, f"No experiment config passed."

    experiment_dirname, dirname_error = get_valid_dirname(experiment['experiment_name'])
    if dirname_error:
        return None, f"Couldn't get a valid dirname for {experiment_name}"

    experiment_dirname = os.path.join(DATA_DIR, experiment_dirname)

    return experiment_dirname, None
