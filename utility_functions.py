import re
import os
import smtplib
import logging
import pandas as pd

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from my_secrets import APP_PASSWORD
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
        logger.warning("No comment passed to extract_ip()")
        return None

    if 'nan' in str(comment).lower():
        return None

    try:
        ips = re.findall(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', comment)
        return ips[0] if ips else None
    except Exception as e:
        logger.error(f"Error when extracting ip from '{comment}': {e}")
        return None
