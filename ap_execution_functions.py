import subprocess
import time
import os
import datetime
import logging
import pandas as pd

from typing import Dict, List, Optional, Tuple
from multiprocessing import Process, Manager
from rich.progress import track
from rich.console import Console
console = Console()

from constants import DEBUG_MODE, SKIP_RESTART, SUMMARISED_DIR
from ess_functions import get_test_name_from_combination_dict, update_ess_df 
from connection_functions import check_connection, ping_machine, check_ssh_connection_with_socket

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

def run_script_on_machine(
        machine_config: Dict = {}, 
        machine_statuses: Dict = {}, 
        timeout_secs: int = 0
    ) -> Optional[str]:

    """
    Run a script on a machine using SSH.

    Params:
        - machine_config (Dict): Machine config.
        - machine_statuses (Dict): Machine statuses.
        - timeout_secs (int): Timeout in seconds.

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
        f"\t\tRunning script on {machine_config['machine_name']} ({machine_config['ip']})."
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
                f"{username}@{machine_ip}"
            )
            logger.debug(
                f"STDOUT:\n\t{stdout}"
            )
            logger.debug(
                f"STDERR:\n\t{stderr}"
            )
            logger.debug(
                f"""
                Script:
                {script_string}
                """
            )

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
            logger.info(
                f"\t\t\tScript on {machine_config['machine_name']} ran successfully."
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

def run_test(
    test_config: Dict = {}, 
    machine_configs: List = [],
    ess_df: pd.DataFrame = pd.DataFrame(),
    experiment_dirpath: str = "",
    noise_gen_config: Dict = {}
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Run the test using the given test config, machine configs and ESS dataframe.

    Params:
        - test_config (Dict): Test config.
        - machine_configs (List): List of machine configs.
        - ess_df (pd.DataFrame): ESS dataframe.
        - experiment_dirpath (str): Experiment directory path.
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

    if experiment_dirpath == "":
        return None, f"No experiment dirpath passed."

    if noise_gen_config is None:
        return None, f"No noise generation config passed."

    if not isinstance(noise_gen_config, Dict):
        return None, f"Noise gen config is not a dict."

    new_ess_df = ess_df

    EXPERIMENT_NAME = os.path.basename(experiment_dirpath)

    test_name, test_error = get_test_name_from_combination_dict(test_config)
    if test_error:
        return None, f"Couldn't get the name of the next test to run."

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

    new_ess_row = {}
    new_ess_row['comments'] = ""

    # 1. Check connections to machine (ping + ssh).
    for machine_config in machine_configs:
        machine_ip = machine_config['ip']
        machine_name = machine_config['machine_name']

        ping_attempts = 3
        while ping_attempts > 0:
            logger.info(
                f"[{4 - ping_attempts}/3] Pinging {machine_name}"
            )
            was_pinged, ping_error = check_connection(machine_config, "ping")    
            if was_pinged:
                break

            ping_attempts -= 1

        if ping_attempts == 0:
            return update_ess_df(
                new_ess_df,
                None,
                None,
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
                f"[{4 - ssh_attempts}/3] SSHing {machine_name}"
            )
            was_sshed, ssh_error = check_connection(machine_config, "ssh")
            if was_sshed:
                break

            ssh_attempts -= 1

        if ssh_attempts == 0:
            return update_ess_df(
                new_ess_df,
                None,
                None,
                test_name,
                0,
                0,
                f"ssh_check_fail",
                test_config,
                {},
                new_ess_row['comments'] + f"Failed to even ssh {machine_ip} after 3 attempts."
            ), f"failed initial ssh check on {machine_ip}"


        # machine_ip = machine_config['ip']
        # ping_response, ping_error = ping_machine(machine_ip)
        # if ping_error:
        #     return update_ess_df(
        #         new_ess_df,
        #         None,
        #         None,
        #         test_name,
        #         0,
        #         0,
        #         f"ping_check_fail",
        #         test_config,
        #         {},
        #         new_ess_row['comments'] + f"Failed to even ping {machine_ip} the first time."
        #     ), f"failed initial ping check on {machine_ip}: {ping_error}"
        #
        # passed_ssh_check, ssh_check_error = check_ssh_connection_with_socket(machine_config)
        # if ssh_check_error:
        #     return update_ess_df(
        #         new_ess_df,
        #         None,
        #         None,
        #         test_name,
        #         1,
        #         0,
        #         f"ssh_check_fail",
        #         test_config,
        #         {},
        #         new_ess_row['comments'] + f"Failed to even ssh {machine_ip} the first time after pinging."
        #     ), f"failed initial ssh check on {machine_ip}: {ssh_check_error}"

    logger.info(
        f"[{EXPERIMENT_NAME}] [{test_name}] Restarting all machines..."
    )

    # 2. Restart machines.
    if not SKIP_RESTART:
        for machine_config in machine_configs:
            logger.info(
                f"\tRestarting {machine_config['machine_name']}..."
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
            f"[{EXPERIMENT_NAME}] [{test_name}] All machines have restarted. Waiting 15 seconds..."
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
        logger.warning(f"Machines failed connection checks.")

        return update_ess_df(
            new_ess_df,
            None,
            None,
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
            f"[{EXPERIMENT_NAME}] [{test_name}] All machines are available."
        )

    if DEBUG_MODE:
        ping_count = 0
        ssh_check_count = 0

    # 4. Get QoS configuration.
    qos_config = test_config

    # 5. Generate test scripts from QoS config.
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
        logger.error(f"Error distributing scripts to machines.")
        return update_ess_df(
            new_ess_df,
            None,
            None,
            test_name,
            ping_count,
            ssh_check_count,
            "script_distribution_fail",
            qos_config,
            {},
            new_ess_row['comments'] + " Failed to distribute scripts across machines."
        ), "failed script distribution"

    if len(scripts_per_machine) == 0:
        logger.error(f"No scripts allocated to machines.")
        return update_ess_df(
            new_ess_df,
            None,
            None,
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
        f"[{EXPERIMENT_NAME}] [{test_name}] Deleting .csv files before test..."
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
        f"[{EXPERIMENT_NAME}] [{test_name}] .csv files deleted"
    )

    # 8. Generate noise genertion scripts if needed and add to existing scripts. 
    if noise_gen_config != {}:
        noise_gen_scripts, noise_gen_error = get_noise_gen_scripts(noise_gen_config)
        if noise_gen_error:
            return update_ess_df(
                new_ess_df,
                None,
                None,
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
                "script_execution_fail",
                qos_config,
                scripts_per_machine,
                new_ess_row['comments'] + " Errors running scripts on machines."
            ), "failed script execution"

    # End timestamp
    end_timestamp = datetime.datetime.now()

    # 10. Check for and download results.
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

    # 11. Confirm all files are downloaded.
    expected_csv_file_count = get_expected_csv_file_count_from_test_name(test_name)
    actual_csv_file_count = get_csv_file_count_from_dir(local_results_dir)

    logger.debug(f"{actual_csv_file_count}/{expected_csv_file_count} downloaded files found.")

    if expected_csv_file_count != actual_csv_file_count:
        return update_ess_df(
            new_ess_df,
            start_timestamp,
            datetime.datetime.now(),
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
        filesize, filesize_error = get_file_size_from_filepath(result_file)
        if filesize_error:
            logger.error(f"Error getting filesize: {filesize_error}")
            continue

        if filesize <= 20:
            return update_ess_df(
                new_ess_df,
                start_timestamp,
                datetime.datetime.now(),
                test_name,
                ping_count,
                ssh_check_count,
                f"empty_file_found",
                qos_config,
                scripts_per_machine,
                new_ess_row['comments'] + f"{result_file} is {filesize} bytes."
            ), f"{result_file} is {filesize} bytes."

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
                                                                                                    
    experiment_name = os.path.basename(dirpath)

    if output_dir_type == "5pi":
        summaries_dirpath = os.path.join(SUMMARISED_DIR, "5pi", experiment_name)
    elif output_dir_type == "3pi":
        summaries_dirpath = os.path.join(SUMMARISED_DIR, "3pi", experiment_name)
    else:
        summaries_dirpath = os.path.join(SUMMARISED_DIR, experiment_name)

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

    experiment_name = os.path.basename(dirpath)
    summaries_dirpath = os.path.join(SUMMARISED_DIR, experiment_name)
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
        f"[{experiment_name}] Generating dataset from {len(test_csvs)} tests with {truncation_percent}% truncation..."
    )

    experiment_name = os.path.basename(dirpath)
    current_timestamp = datetime.datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"{current_timestamp}_{experiment_name}_dataset_{truncation_percent}_percent_truncation.csv"
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
        f"[{experiment_name}] Generated dataset: {filename}"
    )
    return filename
