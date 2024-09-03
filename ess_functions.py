import logging
import os
import pandas as pd

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
            ess_df = pd.read_csv(ess_filepath)
        except Exception as e:
            return None, f"Couldn't ready {ess_filepath}: \n\t{e}"
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

def get_ess_df_from_experiment(experiment_config: Dict = {}) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    if experiment_config == {}:
        return None, "No experiment config passed to get_ess_df_from_experiment()"

    EXPERIMENT_DIRPATH, dirname_error = get_dirname_from_experiment(experiment_config)
    if dirname_error:
        return None, dirname_error

    EXPERIMENT_DIRNAME = os.path.basename(EXPERIMENT_DIRPATH)
    ESS_PATH = os.path.join(ESS_DIR, f"{EXPERIMENT_DIRNAME}.csv")

    ess_df, ess_error = get_ess_df(ESS_PATH)
    if ess_error:
        return None, ess_error

    return ess_df, None

def check_if_ess_rows_match_expected_test_count(experiment_config: Dict = {}) -> Tuple[Optional[bool], Optional[str]]:
    if experiment_config == {}:
        return None, "No experiment config passed."

    ess_df, ess_df_error = get_ess_df_from_experiment(experiment_config)
    if ess_df_error:
        return None, ess_df_error

    ess_row_count = len(ess_df.index)

    expected_test_count, expected_test_count_error = get_expected_test_count_from_experiment(experiment_config)
    if expected_test_count_error:
        return None, expected_test_count_error

    if ess_row_count != expected_test_count:
        return False, None

    return True, None

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
