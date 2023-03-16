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

DEBUG = "[bold blue]DEBUG:[/bold blue]"
WARNING = "[bold red]WARNING:[/bold red]"
ERROR = "[bold red]ERROR:[/bold red]"

def log_debug(message):
    console.print(f"{DEBUG} {message}", style="bold white") if DEBUG_MODE else None

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
        console.print(f"{WARNING} {dirpath_name}_{i-1} already exists. Creating the folder {dirpath} instead.", style="bold white")

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
        return
    
    timer = 0
    while timer < timeout:
        try:
            k = paramiko.RSAKey.from_private_key_file(ssh_key)
            ssh.connect(host, username=username, pkey = k)
            break
        except Exception as e:
            # console.print("[red]Error connecting to " + host + ". Reconnecting...[/red]", style=output_colour)
            # console.print(e, style="bold yellow")
            time.sleep(1)
            timer += 1

    if timer == timeout:
        console.print(f"{ERROR} Timeout after {timeout} seconds when pinging {host}.")
        sys.exit(0)

def validate_ssh_key(ssh_key):
    if not os.path.exists(ssh_key):
        console.print(f"{ERROR} The ssh key file {ssh_key} does NOT exist.", style="bold white")
        return False
    
    try:
        key = paramiko.RSAKey.from_private_key_file(ssh_key)
        return True
    except paramiko.ssh_exception.PasswordRequiredException:
        console.print(f"{ERROR} The ssh key file {ssh_key} requires a password.", style="bold white")
        return False
    except paramiko.ssh_exception.SSHException:
        console.print(f"{ERROR} The ssh key file {ssh_key} is invalid.", style="bold white")
        return False

def restart_machine(ssh, host, username, ssh_key):
    while True:
        try:
            k = paramiko.RSAKey.from_private_key_file(ssh_key)
            ssh.connect(host, username=username, pkey = k)
            ssh.exec_command("sudo reboot")
            time.sleep(3)
            break
        except Exception as e:
            time.sleep(1)

def has_leftovers(machine, ssh):
    k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])
    ssh.connect(machine['host'], username=machine['username'], pkey = k)

    stdin, stdout, stderr = ssh.exec_command(f"ls {machine['home_dir']}/*.csv")

    return len(stdout.readlines()) > 0

def download_leftovers(machine, ssh, testdir):
    log_debug(f"{machine['name']} Checking for leftovers...")

    if has_leftovers(machine, ssh):
        log_debug(f"{machine['name']} Leftovers found.")

        # ? Make leftovers dir.
        leftover_dir = os.path.join(testdir, "leftovers")
        leftover_dir = create_dir(leftover_dir)

        k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])
        ssh.connect(machine['host'], username=machine['username'], pkey=k)

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
                        sftp.get(remote_filesize, local_filepath)
                        download_files_count += 1

            log_debug(f"{machine['name']} {download_files_count} files downloaded.")

    else:
        log_debug(f"{machine['name']} No leftovers found.")

def get_duration_from_test_scripts(scripts):
    if "-executionTime" in scripts:
        return int(scripts.split("-executionTime")[1].split("-")[0])
    else:
        return 0

def start_system_logging(machine, test_title):
    if machine['scripts']:
        duration = get_duration_from_test_scripts(machine['scripts'])
    else:
        duration = get_duration_from_test_name(test_title)

    # ? Give enough buffer time to contain the test.
    duration *= 1.2

    #  ? Check for any leftover logs.
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])

    ssh.connect(machine['host'], username=machine['username'], pkey=k)

    # ? Delete any leftover system logs.
    stdin, stdout, stderr = ssh.exec_command(f"find {machine['home_dir']} -type f \\( -name '*log*' -o -name '*sar_logs*' \\) -delete")

    # ? Start the logging.
    stdin, stdout, stderr = ssh.exec_command("sar -A -o sar_logs 1 " + str(duration) + " >/dev/null 2>&1 &")

def run_scripts(ssh, machine):
    try:
        k = paramiko.RSAKey.from_private_key_file(machine['ssh_key'])
        ssh.connect(machine["host"], username=machine['username'], pkey = k)
        _, stdout, stderr = ssh.exec_command("source ~/.bash_profile;" + machine['scripts'])
    except Exception as e:
        console.print(f"{ERROR} {machine['name']} Error when running scripts. Exception:\n\t{e}", style="bold white")
        return None, str(e)

    return stdout, stderr