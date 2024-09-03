import itertools
import logging
import random

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

def generate_test_config_from_qos(qos: Optional[Dict] = None) -> Optional[Dict]:
    """
    Generate a test config from the given qos settings.

    Params:
        - qos (Dict): QoS settings.

    Returns:
        - Dict: Test config if valid, None otherwise.
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
