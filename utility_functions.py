import fnmatch
import shutil
import sys
import os
import json
import paramiko
import time
import concurrent.futures
import math
import threading
import re
import stat
import multiprocessing

from threading import Thread
from datetime import datetime, timedelta
from pprint import pprint
from itertools import product
from itertools import repeat
from rich.console import Console
from rich.markdown import Markdown
from rich.prompt import Prompt
from rich.prompt import Confirm
from rich.progress import track

# ? Uncomment for rich traceback formatting
# from rich.traceback import install
# install(show_locals=True)

console = Console()

DEBUG_MODE = "debug" in sys.argv
SKIP_RESTART = "skip_restart" in sys.argv

def format_now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

DEBUG = f"[bold blue]DEBUG:[/bold blue]"
WARNING = f"\n\n[bold red]WARNING:[/bold red]"
ERROR = f"[bold red]ERROR:[/bold red]"

def log_debug(message):
    console.print(f"[{format_now()}] {DEBUG} {message}", style="bold white") if DEBUG_MODE else None

def create_dir(dirpath):
    dirpath_name = dirpath

    # ? Make sure that there are no _1 or _n at the end of the name
    index = dirpath.rfind("_")
    if index != -1:
        try:
            int(dirpath[index+1:])
            dirpath_name = dirpath[:index]
        except ValueError:
            # ? No number after the underscore
            None

    i = 0

    while os.path.exists(dirpath):
        i += 1
        dirpath = f"{dirpath_name}_{i}"

    if i > 0:
        console.print(f"[{format_now()}] {WARNING} {dirpath_name}_{i-1} already exists. Creating the folder {dirpath} instead.\n", style="bold white")

    os.mkdir(dirpath)

    return dirpath

def get_combinations(settings):
        return [dict( zip(settings, value)) for value in product(*settings.values()) ];

def get_test_title_from_combination(combination):
    title = ""
    for k, v in combination.items():
        if "bytes" in k:
            title = title + str(v) + "B_"
        elif "pub" in k:
            title = title + str(v) + "P_"
        elif "sub" in k:
            title = title + str(v) + "S_"
        elif "reliability" in k:
            if v:
                title = title + "rel_"
            else:
                title = title + "be_"
        elif "multicast" in k:
            if v:
                title = title + "mc_"
            else: 
                title = title + "uc_"
        elif "latency_count" in k:
            title = title + str(v) + "lc_"
        elif "duration" in k:
            title = title + str(v) + "s_"
        elif "durability" in k:
            title = title + str(v) + "dur_"
            
    return title[:-1]

def get_combination_from_title(title):
    # Example title: 600s_100B_75P_75S_rel_uc_1dur_100lc
    title = title.replace("\n", '') if '\n' in title else title

    combination = {
        "duration_s": None,
        "datalen_bytes": None,
        "pub_count": None,
        "sub_count": None,
        "reliability": None,
        "use_multicast": None,
        "durability": None,
        "latency_count": None
    }

    settings = title.split("_")

    for setting in settings:
        if "s" in setting:
            combination['duration_s'] = int(setting.replace("s", ""))
        elif "B" in setting:
            combination['datalen_bytes'] = int(setting.replace("B", ""))
        elif "P" in setting:
            combination['pub_count'] = int(setting.replace("P", ""))
        elif "S" in setting:
            combination['sub_count'] = int(setting.replace("S", ""))
        elif "uc" in setting or "mc" in setting:
            if "uc" in setting:
                combination["use_multicast"] = False
            else:
                combination["use_multicast"] = True
        elif "rel" in setting or "be" in setting:
            if "rel" in setting:
                combination["reliability"] = True
            else:
                combination["reliability"] = False
        elif "dur" in setting:
            combination['durability'] = int(setting.replace("dur", ""))
        elif "lc" in setting:
            combination['latency_count'] = int(setting.replace("lc", ""))

    return combination

def share(items, bins):
    if len(items) == 0 or bins == 0:
        return []

    if bins == 1:
        return items

    output = []
    
    for i in range(bins):
        output.append([])
    
    while len(items) > 0:
        for i in range(bins):
            try:
                output[i].append(items[0])
                items = items[1:]
            except Exception as e:
                None
            
    return output

def get_duration_from_test_name(testname):
    # ? Look for x numeric digits followed by "s_"
    durations_from_name = re.findall(r'\d*s_', testname)
    
    if len(durations_from_name) == 0:
        return None
    
    duration_from_name = durations_from_name[0]

    duration_from_name = duration_from_name.replace("s_", "")
    
    duration = int(duration_from_name)
    
    return duration

def check_machine_online(ssh, host, username, ssh_key, timeout):
    if not validate_ssh_key(ssh_key):
        return False
    
    timer = 0
    while timer < timeout:
        try:
            k = paramiko.RSAKey.from_private_key_file(ssh_key)
            ssh.connect(host, username=username, pkey = k, banner_timeout=120)
            break
        except Exception as e:
            # console.print("[red]Error connecting to " + host + ". Reconnecting...[/red]", style=output_colour)
            # console.print(e, style="bold yellow")
            time.sleep(1)
            timer += 1

    if timer == timeout:
        console.print(f"[{format_now()}] {ERROR} Timeout after {timeout} seconds when pinging {host}.", style="bold red")
        sys.exit(0)

    return True

def validate_ssh_key(ssh_key):
    if not os.path.exists(ssh_key):
        console.print(f"[{format_now()}] {ERROR} The ssh key file {ssh_key} does NOT exist.", style="bold red")
        return False
    
    try:
        key = paramiko.RSAKey.from_private_key_file(ssh_key)
        return True
    except paramiko.ssh_exception.PasswordRequiredException:
        console.print(f"[{format_now()}] {ERROR} The ssh key file {ssh_key} requires a password.", style="bold red")
        return False
    except paramiko.ssh_exception.SSHException:
        console.print(f"[{format_now()}] {ERROR} The ssh key file {ssh_key} is invalid.", style="bold red")
        return False

def restart_machine(ssh, host, username, ssh_key):
    while True:
        try:
            k = paramiko.RSAKey.from_private_key_file(ssh_key)
            ssh.connect(host, username=username, pkey = k, banner_timeout=120)
            ssh.exec_command("sudo reboot")
            time.sleep(3)
            break
        except Exception as e:
            time.sleep(1)

def has_leftovers(machine, ssh):
    k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])
    try:
        ssh.connect(machine['host'], username=machine['username'], pkey = k, banner_timeout=120)
        
        stdin, stdout, stderr = ssh.exec_command(f"ls {machine['home_dir']}/*.csv")
        
        return len(stdout.readlines()) > 0
    
    except Exception as e:
        # TODO: Write to exceptions log.
        return False

def download_leftovers(machine, ssh, testdir):
    log_debug(f"{machine['name']} Checking for leftovers...")

    if has_leftovers(machine, ssh):
        log_debug(f"{machine['name']} Leftovers found.")

        # ? Make leftovers dir.
        leftover_dir = os.path.join(testdir, "leftovers")
        if not os.path.exists(leftover_dir):
            try:
                os.makedirs(leftover_dir)
            except FileExistsError:
                None

        k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])

        try:
            ssh.connect(machine['host'], username=machine['username'], pkey=k, banner_timeout=120)
        
            with ssh.open_sftp() as sftp:
                remote_dir = machine['home_dir']
                local_dir = leftover_dir

                csv_files = sftp.listdir(remote_dir)

                download_files_count = 0

                for csv_file in csv_files:
                    if csv_file.endswith(".csv"):
                        remote_filepath = os.path.join(remote_dir, csv_file)
                        local_filepath = os.path.join(local_dir, csv_file)
                        remote_filesize = sftp.stat(remote_filepath).st_size

                        if remote_filesize > 0:
                            sftp.get(remote_filepath, local_filepath)
                            sftp.remove(remote_filepath)
                            download_files_count += 1

                log_debug(f"{machine['name']} {download_files_count} leftover files downloaded to {local_dir}.")
                return True

        except Exception as e:
            # TODO: Write exception to exception log
            return False

    else:
        log_debug(f"{machine['name']} No leftovers found.")
        return True

def get_duration_from_test_scripts(scripts):
    if "-executionTime" in scripts:
        return int(scripts.split("-executionTime")[1].split("-")[0])
    else:
        return 0

def start_system_logging(machine, test_title, buffer_multiple):
    script_len = len(machine["scripts"].replace("source ~/.bashrc;", ""))

    if script_len > 10:
        duration = get_duration_from_test_scripts(machine['scripts'])
    else:
        duration = get_duration_from_test_name(test_title)

    # ? Give enough buffer time to contain the test.
    duration *= buffer_multiple
    log_debug(f"{machine['name']} Started logging for {duration} seconds.")

    #  ? Check for any leftover logs.
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])

    try:
        ssh.connect(machine['host'], username=machine['username'], pkey=k, banner_timeout=120)
    
        # ? Delete any leftover system logs.
        stdin, stdout, stderr = ssh.exec_command(f"find {machine['home_dir']} -type f \\( -name '*log*' -o -name '*sar_logs*' \\) -delete")
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            console.print(f"[{format_now()}] {ERROR} {machine['name']} Error when executing:\n\tfind {machine['home_dir']} -type f \\( -name '*log*' -o -name '*sar_logs*' \\) -delete", style="bold red")

        # ? Start the logging.
        stdin, stdout, stderr = ssh.exec_command("sar -A -o sar_logs 1 " + str(int(duration)) + " >/dev/null 2>&1 &")
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            console.print(f"[{format_now()}] {ERROR} {machine['name']} Error when executing:\n\tsar -A -o sar_logs 1 " + str(duration) + " >/dev/null 2>&1 &", style="bold red")

        # ? Wait some time for the file to be made.
        time.sleep(5)

        # ? Check that log is working - file should be generated.
        try:
            log_dir = os.path.join(f"{machine['home_dir']}", "sar_logs")
            sftp = ssh.open_sftp()
            sftp.stat(log_dir)
        except IOError:
            return False

        return True
    
    except Exception as e:
        return False

def run_scripts(ssh, machine):
    try:
        k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])
        ssh.connect(machine["host"], username=machine['username'], pkey = k, banner_timeout=120)
        _, stdout, stderr = ssh.exec_command(f"{machine['scripts']}")

        # ? Wait for the scripts to finish.
        output = stdout.readlines()
        error = stderr.readlines()
        
        if len(output) > 0:
            log_debug(f"{machine['name']} stdout has content.")
            # log_debug(f"{machine['name']} Output:\n\t{output}")

        if len(error) > 0:
            log_debug(f"{machine['name']} stderr has content.")
            # log_debug(f"{machine['name']} Error:")
            # for line in error:
            #     console.print(f"\t{line}", style="bold red")

        return stdout, stderr
    
    except Exception as e:
        console.print(f"[{format_now()}] {ERROR} {machine['name']} Error when running scripts. Exception:\n\t{e}", style="bold red")
        return None, str(e)


def get_expected_csv_count_from_test_title(test_title):
    sub_count = int(test_title.split('_')[3][:-1])
    csv_count = sub_count + 1

    return csv_count

def download_csv_files(machine, ssh, testdir):
    k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])
    
    try:
        ssh.connect(machine['host'], username=machine['username'], pkey=k, banner_timeout=120)
    
        with ssh.open_sftp() as sftp:
            remote_dir = machine['home_dir']
            local_dir = testdir

            csv_files = sftp.listdir(remote_dir)

            download_files_count = 0

            for csv_file in csv_files:
                if csv_file.endswith(".csv"):
                    remote_filepath = os.path.join(remote_dir, csv_file)
                    local_filepath = os.path.join(local_dir, csv_file)
                    remote_filesize = sftp.stat(remote_filepath).st_size

                    if remote_filesize > 0:
                        sftp.get(remote_filepath, local_filepath)
                        sftp.remove(remote_filepath)
                        download_files_count += 1

        return download_files_count
    except Exception as e:
        # TODO: Write to exceptions log.
        return 0

def download_logs(machine, ssh, logs_dir):
    downloaded_files_count = 0

    with ssh.open_sftp() as sftp:
        sar_logs = [_ for _ in sftp.listdir(machine['home_dir']) if "sar_logs" in _ and "log" in _]

        if len(sar_logs) == 0:
            console.print(f"[{format_now()}] {ERROR} {machine['name']} No logs found.", style="bold red")
            return 0
        
        elif len(sar_logs) > 1:
            log_debug(f"{machine['name']} Multiple sar_logs found.")
            
            # ? Get the most recently created file.
            latest_file = None
            latest_time = 0

            for sar_log in sar_logs:
                if sar_log.st_ctime > latest_time:
                    latest_file = sar_log
                    latest_time = sar_log.st_ctime

            sar_log = latest_file
        else:
            sar_log = sar_logs[0]

        # ? Parse CPU stats
        stdin, stdout, stderr = ssh.exec_command("sar -f " + sar_log + " > cpu.log")
        if stdout.channel.recv_exit_status() == 0:
            log_debug(f"{machine['name']} CPU logs parsed.")
        else:
            log_debug(f"{machine['name']} Error parsing CPU logs.")
        
        # ? Parse memory stats
        stdin, stdout, stderr = ssh.exec_command("sar -r -f " + sar_log + " > mem.log")
        if stdout.channel.recv_exit_status() == 0:
            log_debug(f"{machine['name']} MEM logs parsed.")
        else:
            log_debug(f"{machine['name']} Error parsing MEM logs.")
        
        # ? Parse network stats
        network_options = ["DEV", "EDEV", "NFS", "NFSD", "SOCK", "IP", "EIP", "ICMP", "EICMP", "TCP", "ETCP", "UDP", "SOCK6", "IP6", "EIP6", "ICMP6", "EICMP6", "UDP6"]
        for option in network_options:
            stdin, stdout, stderr = ssh.exec_command("sar -n " +option+ " -f " + sar_log + " > " +option.lower()+ ".log")
            if stdout.channel.recv_exit_status() != 0:
                log_debug(f"{machine['name']} Error parsing NETWORK logs.")

        log_debug(f"{machine['name']} NETWORK logs parsed.")

        # ? Delete the original unparsed sar log file - we no longer have a use for it
        sftp.remove(sar_log)

        expected_logs = ["cpu.log", "mem.log"] + [x.lower() + ".log" for x in network_options]
        found_logs = [x for x in sftp.listdir(machine['home_dir']) if '.log' in x]
        
        if len(expected_logs) != len(found_logs):
            log_debug(f"{machine['name']} Mismatch found between expected and found logs.\n\t {(len(expected_logs))} expected logs.\n\t {str(len(found_logs))} logs founds.")
            
            if len(expected_logs) > len(found_logs):
                console.print(machine['name'] + ": " + "You are missing the following logs:", style="bold red") if DEBUG_MODE else None
                for log in list(set(expected_logs) - set(found_logs)):
                    console.print("\t" + log, style="bold red")
            else:
                console.print(machine['name'] + ": " + "You have the following extra logs:", style="bold red") if DEBUG_MODE else None
                for log in list(set(found_logs) - set(expected_logs)):
                    console.print("\t" + log, style="bold red")
        else:
            for log in expected_logs:
                sftp.get(log, os.path.join(logs_dir, f"{machine['name']}_{log}"))
                downloaded_files_count += 1
                sftp.remove(log)
                
        leftover_logs = [x for x in sftp.listdir(machine['home_dir']) if '.log' in x]

        if len(leftover_logs) > 0:
            console.print(f"[{format_now()}] {WARNING} {machine['name']} Some logs were leftover.\n", style="bold white")

    return downloaded_files_count

def update_progress(progress_json, test_title, start_time, end_time, test_end_status):
    start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
    end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
    
    with open(progress_json, 'r+') as file:
        data = json.load(file)
        data.append({'test': test_title, 'start_time': start_time, 'end_time': end_time, 'status': test_end_status})
        file.seek(0)
        json.dump(data, file, indent=4)

def validate_setting(combination_value, setting_string, scripts, combination):
    instances = []
    
    for script in scripts:
        script_settings = script.split(" -")
        instances += [_ for _ in script_settings if setting_string in _]
        
    if len(instances) == 0:
        console.print(f"[{format_now()}] {ERROR} {setting_string} not found in the generated scripts...", style="bold red")
        console.print(f"\nCombinations:", style="bold white")
        for key, value in combination.items():
            console.print(f"\t{key}: {value}", style="bold white")
        console.print(f"\n", style="bold white")
        console.print(f"Scripts:", style="bold white")
        for script in scripts:
            console.print(f"\t{script}", style="bold white")
        console.print(f"\n", style="bold white")
        return False

    # ? Get the dataLen values to compare with datalen_bytes.
    values = []
    for item in instances:
        try:
            value = int(item.replace(f"-{setting_string} ", ""))
        except ValueError:
            value = int(item.replace(f"{setting_string} ", ""))
        values.append(value)

    if len(set(values)) > 1:
        # ! Multiple datalen values found in the script when there should be one.
        console.print(f"[{format_now()}] {ERROR} Multiple {setting_string} values found in the script where there should be one.\n{scripts}", style="bold red")
        console.print(f"\nCombinations:", style="bold white")
        for key, value in combination.items():
            console.print(f"\t{key}: {value}", style="bold white")
        console.print(f"\n", style="bold white")
        console.print(f"Scripts:", style="bold white")
        for script in scripts:
            console.print(f"\t{script}", style="bold white")
        console.print(f"\n", style="bold white")
        return False
    elif len(set(values)) == 0:
        # ! No datalen values found in the script when there should be one.
        console.print(f"[{format_now()}] {ERROR} No {setting_string} values found in the script when there should be one.\n{scripts}", style="bold red")
        console.print(f"\nCombinations:", style="bold white")
        for key, value in combination.items():
            console.print(f"\t{key}: {value}", style="bold white")
        console.print(f"\n", style="bold white")
        console.print(f"Scripts:", style="bold white")
        for script in scripts:
            console.print(f"\t{script}", style="bold white")
        console.print(f"\n", style="bold white")
        return False
    else:
        if combination_value != values[0]:
            # ! The datalen in the combination and the data len used in the script are two different values.
            console.print(f"The {setting_string} in the combination and the {setting_string} used in the script are two different values.\n{scripts}", style="bold red")
            console.print(f"\nCombinations:", style="bold white")
            for key, value in combination.items():
                console.print(f"\t{key}: {value}", style="bold white")
            console.print(f"\n", style="bold white")
            console.print(f"Scripts:", style="bold white")
            for script in scripts:
                console.print(f"\t{script}", style="bold white")
            console.print(f"\n", style="bold white")
            return False

    return True

