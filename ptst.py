import filecmp
import colorsys
import random
import itertools
import argparse
import os
import json
import sys
import time
import subprocess
import signal
import zipfile

from pprint import pprint
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.markdown import Markdown
from multiprocessing import Process, Manager

console = Console()

def write_permutations_to_file(file, permutations):
    console.print(f"Writing permutations to {file}.", style="bold white")
    with open(file, 'w') as f:
        f.write("")
    
    for permutation in permutations:
        perm_name = generate_permutation_name(permutation)
        with open(file, "a") as f:
            f.write(f"{perm_name} \n")

def generate_color_list(color_count):
    colors = []
    for i in range(color_count):
        # Generate a random hue and saturation
        hue = random.random()
        saturation = random.uniform(0.8, 1.0)
        
        # Set the value to maximum
        value = 1.0
        
        # Convert the HSV values to RGB values
        r, g, b = tuple(round(i * 255) for i in colorsys.hsv_to_rgb(hue, saturation, value))
        
        # Add the color to the list
        colors.append(f"#{r:02X}{g:02X}{b:02X}")
    
    return colors

COLORS = generate_color_list(100)

def pick_contrasting_color(color):
    # Convert the color to RGB values
    r, g, b = tuple(int(color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
    
    # Calculate the luminance of the color
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    
    # Pick a random color that has a contrasting luminance
    while True:
        new_color = random.choice(COLORS)
        new_r, new_g, new_b = tuple(int(new_color.lstrip('#')[i:i+2], 16) for i in (0, 2, 4))
        new_luminance = (0.299 * new_r + 0.587 * new_g + 0.114 * new_b) / 255
        if abs(new_luminance - luminance) > 0.5:
            return new_color

def generate_random_permutation(settings):
    permutation = {}
    for key, value in settings.items():
        is_boolean = value == [True, False] or value == [False, True]
        if len(value) == 2 and isinstance(value[0], int) and isinstance(value[1], int) and not is_boolean:
            if value[0] > value[1]:
                raise ValueError(f"Setting {key} has invalid range: {value}")
            permutation[key] = random.randint(value[0], value[1])
        elif is_boolean:
            permutation[key] = random.choice(value)
        else:
            permutation[key] = value[0]
    
    return tuple(permutation.values())

def get_machine_response_statuses(machines):
    machine_statuses = []

    for machine in machines:
        name = machine['name']
        host = machine['host']
        username = machine['username']
        ssh_key = machine['ssh_key']
        
        response = os.system(f"ping -c 1 {host} > /dev/null 2>&1")
        
        if response == 0:
            ping_response = True
            # ? Check if machine is accessible via SSH
            ssh_command = f"ssh -i {ssh_key} {username}@{host} 'echo 1' > /dev/null 2>&1"
            ssh_response = subprocess.run(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if ssh_response.returncode == 0:
                ssh_response = True
            else:
                ssh_response = False
                
        else:
            ping_response = False
            ssh_response = False
            
        machine_statuses.append({
            'name': name,
            'host': host,
            'ping_response': ping_response,
            'ssh_response': ssh_response
        })
        
    return machine_statuses

def zip_folder(folder_path):
    output_path = folder_path + ".zip"
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, os.path.relpath(file_path, folder_path))

def ping_machine(machine):
    ping_command = f"ping -c 1 {machine['host']}"
    process = subprocess.run(ping_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = process.stdout.decode()
    return "1 received" in output

def test_ssh(machine):
    ssh_command = f"ssh -o BatchMode=yes {machine['host']} echo ok"
    process = subprocess.run(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = process.stdout.decode()
    if "ok" in output:
        return True
    else:
        return False

def ssh_to_machine(machines, machine, script_string, timeout, machine_statuses, test_name, campaign_folder, color, max_retries=20):
    status = {
        "host": machine['host'],
        "name": machine['name'],
        "status": "",
        "time_to_scripts_s": 0,
        "pings": 0,
        "ssh_pings": 0
    }    
    
    machine_name = machine['name']
    test_folder = os.path.join(campaign_folder, test_name)
    
    console.print(f"{machine_name} Restarting machine...", style="bold " + color)
    # ? Restart the machine.
    ssh_command = f"ssh {machine['username']}@{machine['host']} 'sudo reboot'"
    with open(os.devnull, 'w') as devnull:
        subprocess.run(ssh_command, shell=True, stdout=devnull, stderr=devnull)
        
    # ? Wait some time for restart to happen.
    time.sleep(5)
    
    start_time = time.time()
    
    for i in range(max_retries):
        console.print(f"Pinging {machine_name} (attempt {i+1}/{max_retries})...", style="bold " + color)
        if ping_machine(machine):
            status['pings'] = i + 1
            break
        if i == max_retries - 1:
            status['status'] = status["status"] + "Unreachable."
            machine_statuses.append(status)
            return None, None
        time.sleep(1)
        
    for i in range(max_retries):
        console.print(f"SSH Pinging {machine_name} (attempt {i+1}/{max_retries})...", style="bold " + color)
        if test_ssh(machine):
            status['ssh_pings'] = i + 1
            break
        if i == max_retries - 1:
            status['status'] = status["status"] + "Unreachable."
            machine_statuses.append(status)
            return None, None
        time.sleep(1)
    
    # ? Ping other machines to make sure they are good to go.
    other_machines = [m for m in machines if m['name'] != machine_name]
    for other_machine in other_machines:
        other_machine_name = other_machine['name']
        for i in range(max_retries):
            console.print(f"{machine_name}: Pinging {other_machine_name} (attempt {i+1}/{max_retries})...", style="bold " + color)
            if ping_machine(other_machine):
                break
            if i == max_retries - 1:
                status['status'] = status["status"] + f"{machine_name}: {other_machine_name} unreachable via ping. "
                machine_statuses.append(status)
                return None, None
            time.sleep(1)
            
    # ? SSH Ping other machines to make sure they are good to go.
    other_machines = [m for m in machines if m['name'] != machine_name]
    for other_machine in other_machines:
        other_machine_name = other_machine['name']
        for i in range(max_retries):
            console.print(f"{machine_name}: SSH Pinging {other_machine_name} (attempt {i+1}/{max_retries})...", style="bold " + color)
            if test_ssh(other_machine):
                break
            if i == max_retries - 1:
                status['status'] = status['status'] + f"{machine_name}: {other_machine_name} unreachable via ssh. "
                machine_statuses.append(status)
                return None, None
            time.sleep(1)
    
    delete_logs_command = f"find {machine['home_dir']} -type f \( -name \"*.log\" -o -name \"*sar_logs*\" \) -delete"
    start_logs_command = f"sar -A -o sar_logs 1 {timeout} >/dev/null 2>&1 &"
    
    # ? Delete all logs on remote machine
    console.print(f"{machine_name}: Deleting leftover logs...", style="bold " + color)
    ssh_command = f"ssh {machine['username']}@{machine['host']} \'{delete_logs_command}\'"
    process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        console.print(f"{machine_name}: Delete logs failed", style="bold red")
        console.print(ssh_command)
        console.print(stdout)
        console.print(stderr)
        status['status'] = status["status"] + "Delete logs failed. "
        machine_statuses.append(status)
        return None, None
    
    # ? Start logging on remote machine
    console.print(f"{machine_name}: Starting system logs...", style="bold " + color)
    ssh_command = f"ssh {machine['username']}@{machine['host']} \'{start_logs_command}\'"
    process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        console.print(f"{machine_name}: Start logs failed", style="bold red")
        console.print(stdout)
        console.print(stderr)
        status['status'] = status['status'] + "Start logs failed. "
        machine_statuses.append(status)
        return None, None

    script_time = time.time()

    time_to_scripts_s = script_time - start_time
    
    status['time_to_scripts_s'] = int(time_to_scripts_s)
        
    # ? Run scripts on remote machine
    console.print(f"{machine_name}: Running scripts...", style="bold " + color)
    ssh_command = f"ssh {machine['username']}@{machine['host']} '{script_string}'"
    process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    start_time = time.time()
    while True:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            status['status'] = status['status'] + "Punctual. "
            break
        elif time.time() - start_time > timeout:
            process.kill()
            stdout, stderr = process.communicate()
            status['status'] = status['status'] + "Script prolonged. "
            status[f"{machine_name}_script"] = script_string
            break
        time.sleep(1)

    end_time = time.time()
    status['script_exec_s'] = int(end_time - start_time)

    # ? Start timer after scripts are done
    start_time = time.time()

    if "punctual" in status['status'].lower():
        remote_files_command = f"ssh {machine['username']}@{machine['host']} 'ls *.csv'"
        remote_files_process = subprocess.run(remote_files_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        remote_files = remote_files_process.stdout.decode().split()
        
        status['csv_count'] = len(remote_files)
        
        if len(remote_files) == 0:
            status['status'] = status['status'] + "No csv files found. "
        else:
            os.makedirs(test_folder, exist_ok=True)
            
            console.print(f"{machine_name}: Downloading csv files...", style="bold " + color)
            for remote_file in remote_files:
                remote_path = remote_file
                local_path = os.path.join(test_folder, f"{remote_file}")
                scp_command = f"scp {machine['username']}@{machine['host']}:{remote_path} {local_path}"
                with open(os.devnull, 'w') as devnull:
                    subprocess.run(scp_command, shell=True, stdout=devnull, stderr=devnull)
            
            console.print(f"{machine_name}: Deleting csv files...", style="bold " + color)
            # ? Delete all csv files on remote machine
            ssh_command = f"ssh {machine['username']}@{machine['host']} 'rm *.csv'"
            with open(os.devnull, 'w') as devnull:
                subprocess.run(ssh_command, shell=True, stdout=devnull)
    else:
        status['csv_count'] = 0

    parse_cpu_logs_command = f"sar -f sar_logs > cpu.log"
    parse_mem_logs_command = f"sar -r -f sar_logs > mem.log"
    network_options = ["DEV", "EDEV", "NFS", "NFSD", "SOCK", "IP", "EIP", "ICMP", "EICMP", "TCP", "ETCP", "UDP", "SOCK6", "IP6", "EIP6", "ICMP6", "EICMP6", "UDP6"]
    
    # ? Parse cpu logs on remote machine
    console.print(f"{machine_name}: Parsing cpu logs...", style="bold " + color)
    ssh_command = f"ssh {machine['username']}@{machine['host']} \'{parse_cpu_logs_command}\'"
    process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        console.print(f"{machine_name}: Parse CPU logs failed", style="bold red")
        console.print(ssh_command)
        console.print(stdout)
        console.print(stderr)
        status['status'] = status['status'] + "Parse cpu logs failed. "
    
    # ? Parse memory logs on remote machine
    console.print(f"{machine_name}: Parsing mem logs...", style="bold " + color)
    ssh_command = f"ssh {machine['username']}@{machine['host']} \'{parse_mem_logs_command}\'"
    process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        console.print(f"{machine_name}: Parse MEM logs failed", style="bold red")
        console.print(stdout)
        console.print(stderr)
        status['status'] = status['status'] + "Parse mem logs failed. "

    # ? Parse network logs on remote machine
    console.print(f"{machine_name}: Parsing network logs...", style="bold " + color)
    for network_option in network_options:
        parse_command = f"sar -n {network_option} -f sar_logs > {network_option.lower()}.log"
        ssh_command = f"ssh {machine['username']}@{machine['host']} \'{parse_command}\'"
        process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            console.print(f"{machine_name}: Parse {network_option} logs failed", style="bold red")
            console.print(stdout)
            console.print(stderr)
            status['status'] = status['status'] + f"Parse {network_option.lower()} logs failed. "

    # ? Download all .logs from remote machine
    remote_files_command = f"ssh {machine['username']}@{machine['host']} 'ls *.log'"
    remote_files_process = subprocess.run(remote_files_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    remote_files = remote_files_process.stdout.decode().split()
    
    status['log_count'] = len(remote_files)
    
    if len(remote_files) == 0:
        status['status'] = status['status'] + "No log files. "
    else:
        os.makedirs(test_folder, exist_ok=True)
        
        console.print(f"{machine_name}: Downloading log files...", style="bold " + color)
        for remote_file in remote_files:
            remote_path = remote_file
            local_path = os.path.join(test_folder, f"{remote_file}")
            # console.print(f"{machine_name}: Downloading: {remote_path} -> {local_path}", style="bold " + color)
            scp_command = f"scp {machine['username']}@{machine['host']}:{remote_path} {local_path}"
            with open(os.devnull, 'w') as devnull:
                process = subprocess.run(scp_command, shell=True, stdout=devnull, stderr=subprocess.PIPE)
                if process.stderr:
                    console.print(f"{machine_name}: Downloading {remote_file} failed", style="bold red")
                    console.print(process.stderr)
                    continue
            
            # ? Rename the file to include the machine name
            os.rename(local_path, os.path.join(test_folder, f"{machine_name}_{remote_file}"))
        
        console.print(f"{machine_name}: Deleting log files...", style="bold " + color)
        # ? Delete all log files on remote machine
        ssh_command = f"ssh {machine['username']}@{machine['host']} 'rm *.log'"
        with open(os.devnull, 'w') as devnull:
            subprocess.run(ssh_command, shell=True, stdout=devnull)

    # ? Look for logs in the test_folder.
    downloaded_log_files = [file for file in os.listdir(test_folder) if file.endswith(".log")]
    if len(downloaded_log_files) == 0:
        status['status'] = status['status'] + f"No log files downloaded. "

    # ? End timer for post script execution
    end_time = time.time()
    status['post_script_time_s'] = int(end_time - start_time)

    machine_statuses.append(status)

    return stdout, stderr

def generate_scripts(permutation):
    script_base = ""
    
    pub_count = permutation[2]
    sub_count = permutation[3]
    
    duration_output = ""
    latency_count_output = ""
    
    script_base = script_base + "-dataLen " + str(permutation[1]) + " "
    
    if not permutation[4]:
        script_base = script_base + "-bestEffort "
    
    if permutation[5]:
        script_base = script_base + "-multicast "
    
    script_base = script_base + "-durability " + str(permutation[6]) + " "
    
    latency_count_output = "-latencyCount " + str(permutation[7]) + " "
    
    duration_output = "-executionTime " + str(permutation[0])
    
    scripts = []
    
    if pub_count == 1:
        scripts.append( script_base + "-pub -outputFile pub_0.csv -numSubscribers " + str(sub_count))
    elif pub_count == 0:
        console.print("Publisher count can't be 0.", style="bold red")
        sys.exit(0)
    else:
        sub_count_string = "-numSubscribers " + str(sub_count) + " "
        for i in range(pub_count):
            if i == 0:
                scripts.append(script_base + "-pub -pidMultiPubTest " + str(i) + " -outputFile pub_" +str(i)+ ".csv " + sub_count_string)
            else:
                scripts.append(script_base + "-pub -pidMultiPubTest " + str(i) + " " + sub_count_string)
    
    if sub_count == 1:
        scripts.append( script_base + "-sub -outputFile sub_0.csv -numPublishers " + str(pub_count))
    elif sub_count == 0:
        console.print("Subscriber count can't be 0.", style="bold red")
        sys.exit(0)
    else:
        pub_count_string = "-numPublishers " + str(pub_count) + " "
        for i in range(sub_count):
            scripts.append(script_base + "-sub -sidMultiSubTest " + str(i) + " -outputFile sub_" + str(i) + ".csv " + pub_count_string)
    
    updated_scripts = []
    for script in scripts:
        if "-pub" in script:
            if duration_output:
                script = script + " " + duration_output
            if latency_count_output:
                script = script + " " + latency_count_output
            script = script + " -batchSize 0 "
            
        script = script + " -transport UDPv4 "
            
        updated_scripts.append(script)
           
    return updated_scripts

def parse_permutation_name(name):
    parts = name.split('_')
    duration = int(parts[0][:-3])
    datalen = int(parts[1][:-1])
    pub_count = int(parts[2][:-1])
    sub_count = int(parts[3][:-1])
    reliability = parts[4] == "REL"
    use_multicast = parts[5] == "MC"
    durability = int(parts[6][:-3])
    latency_count = int(parts[7][:-2])
    return (duration, datalen, pub_count, sub_count, reliability, use_multicast, durability, latency_count)

def generate_permutation_name(permutation):
    duration_s = f"{permutation[0]}SEC"
    datalen_bytes = f"{permutation[1]}B"
    pub_count = f"{permutation[2]}P"
    sub_count = f"{permutation[3]}S"
    reliability = "REL" if permutation[4] else "BE"
    use_multicast = "MC" if permutation[5] else "UC"
    durability = f"{permutation[6]}DUR"
    latency_count = f"{permutation[7]}LC"
    return f"{duration_s}_{datalen_bytes}_{pub_count}_{sub_count}_{reliability}_{use_multicast}_{durability}_{latency_count}"

def validate_config(config_data):
    # Check if config_data is a list
    if not isinstance(config_data, list) or len(config_data) == 0 or not isinstance(config_data[0], dict):
        console.print("Error: config file must contain a list with at least a dictionary element.")
        return False

    # Check if required keys are present in the dictionary
    required_keys = ['name', 'settings', 'custom_tests_file', 'machines']
    for key in required_keys:
        if key not in config_data[0]:
            console.print(f"Error: {key} key is missing from config file.")
            return False

    # Check if machines is a list of dictionaries
    if not isinstance(config_data[0]['machines'], list) or not all(isinstance(machine, dict) for machine in config_data[0]['machines']):
        console.print("Error: machines key must contain a list of dictionaries.")
        return False

    # Check if required keys are present in each machine dictionary
    machine_required_keys = ['name', 'host', 'ssh_key', 'username', 'home_dir', 'perftest', 'participant_allocation']
    for machine in config_data[0]['machines']:
        for key in machine_required_keys:
            if key not in machine:
                console.print(f"Error: {key} key is missing from machine dictionary in config file.")
                return False

    # Check if random_combination_generation is a boolean if present
    if 'random_combination_generation' in config_data[0] and not isinstance(config_data[0]['random_combination_generation'], bool):
        console.print("Error: random_combination_generation key must be a boolean.")
        return False

    # Check if random_combination_count is an integer if present
    if 'random_combination_count' in config_data[0] and not isinstance(config_data[0]['random_combination_count'], int):
        console.print("Error: random_combination_count key must be an integer.")
        return False

    # Check if settings values exceed a list length of 2 if random combination generation is enabled
    if config_data[0].get('random_combination_generation', False):
        for setting in config_data[0]['settings']:
            if len(config_data[0]['settings'][setting]) > 2:
                console.print(f"Error: {setting} setting cannot have more than 2 values if random combination generation is enabled.")
                return False

    # All checks passed
    return True

def distribute_scripts(scripts, machines):
    pub_scripts = [script for script in scripts if "-pub" in script]
    sub_scripts = [script for script in scripts if "-sub" in script]
    
    pub_machines = [machine for machine in machines if machine['participant_allocation'] in ['pub', 'all']]
    sub_machines = [machine for machine in machines if machine['participant_allocation'] in ['sub', 'all']]
    
    pub_scripts_per_machine = len(pub_scripts) // len(pub_machines)
    sub_scripts_per_machine = len(sub_scripts) // len(sub_machines)
    
    pub_scripts_remainder = len(pub_scripts) % len(pub_machines)
    sub_scripts_remainder = len(sub_scripts) % len(sub_machines)
    
    pub_scripts_index = 0
    sub_scripts_index = 0
    
    scripts_per_machine_list = []
    
    for i, machine in enumerate(pub_machines):
        perftest_path = machine.get('perftest', '')
        if perftest_path.endswith(';'):
            perftest_path = perftest_path[:-1]
        scripts = ["source ~/.bashrc;"]
        for j in range(pub_scripts_per_machine):
            script = pub_scripts[pub_scripts_index]
            pub_scripts_index += 1
            script = perftest_path + ' ' + script
            script = script[:script.find(';')+1] + script[script.find(';')+1:].replace(';', ' & ')
            scripts.append(script)
        
        if pub_scripts_remainder > 0:
            script = pub_scripts[pub_scripts_index]
            pub_scripts_index += 1
            script = perftest_path + ' ' + script
            script = script[:script.find(';')+1] + script[script.find(';')+1:].replace(';', ' & ')
            scripts.append(script)
            pub_scripts_remainder -= 1
        
        script_string = ';'.join(scripts)
        script_string = script_string.replace(';;', ';')
        script_string = script_string[:script_string.find(';')+1] + script_string[script_string.find(';')+1:].replace(';', ' & ')
        scripts_per_machine_list.append(script_string)
    
    for i, machine in enumerate(sub_machines):
        perftest_path = machine.get('perftest', '')
        if perftest_path.endswith(';'):
            perftest_path = perftest_path[:-1]
        scripts = ["source ~/.bashrc;"]
        for j in range(sub_scripts_per_machine):
            script = sub_scripts[sub_scripts_index]
            sub_scripts_index += 1
            script = perftest_path + ' ' + script
            script = script[:script.find(';')+1] + script[script.find(';')+1:].replace(';', ' & ')
            scripts.append(script)
        
        if sub_scripts_remainder > 0:
            script = sub_scripts[sub_scripts_index]
            sub_scripts_index += 1
            script = perftest_path + ' ' + script
            script = script[:script.find(';')+1] + script[script.find(';')+1:].replace(';', ' & ')
            scripts.append(script)
            sub_scripts_remainder -= 1
        
        script_string = '&'.join(scripts)
        script_string = script_string.replace(';;', ';')
        script_string = script_string[:script_string.find(';')+1] + script_string[script_string.find(';')+1:].replace(';', ' & ')
        script_string = script_string.replace(";&", ";")
        scripts_per_machine_list.append(script_string)
    
    return scripts_per_machine_list

def main():
    parser = argparse.ArgumentParser(description='Read in filepath to config file and buffer duration in seconds.')
    parser.add_argument('config_file', type=str, help='Filepath to config file.')
    parser.add_argument('buffer_duration', type=int, help='Buffer duration in seconds.')
    args = parser.parse_args()

    config_file = args.config_file
    buffer_duration = args.buffer_duration

    if not os.path.exists(config_file):
        print(f"Error: {config_file} does not exist.")
        exit()

    if buffer_duration <= 0:
        print("Error: buffer duration must be an integer value bigger than 0.")
        exit()

    with open(config_file, 'r') as f:
        config_data = json.load(f)

    if not validate_config(config_data):
        exit()

    # Loop through each campaign in the config file and create a folder for that campaign
    for campaign in config_data:
        resumption_mode = False
        campaign_name = campaign['name']
        campaign_folder = campaign_name.lower().replace(' ', '_')
        statuses_file = campaign_name.lower().replace(" ", "_") + "_statuses.json"
        
        # ? Check if the folder already exists and enter resumption mode
        if os.path.exists(campaign_folder):
            resumption_mode = True
            # ? Get the last status from the statuses file
            with open(statuses_file, 'r') as f:
                statuses = f.read()
            statuses = ", ".join(statuses.split(",")[:-1]) + "]"
            statuses = json.loads(statuses)
            last_status = statuses[-1]
            last_permutation = parse_permutation_name(last_status['permutation_name'])
            console.print(f"{campaign_folder} already exists. Resuming {campaign_name} from {last_status['permutation_name']} instead of running from the start.", style="bold green")
        else:
            os.makedirs(campaign_folder, exist_ok=True)
            # ? Start status file with [
            with open(statuses_file, 'w') as f:
                f.write('[')
        
        # Generate all possible permutations of settings values if random combination generation is disabled
        if not campaign.get('random_combination_generation', False):
            # ? USING PRESET COMBINATION GENERATION
            camp_index = config_data.index(campaign)
            console.print(Markdown(f"# [{camp_index + 1}/{len(config_data)}] Running Campaign: {campaign_name}"))
            
            # ? Check for custom_tests_file
            custom_tests_file = campaign.get('custom_tests_file', '')
            
            if custom_tests_file != '':
                # ? Check if file path exists
                if not os.path.exists(custom_tests_file):
                    console.print(f"Error: {custom_tests_file} does not exist.")
                    sys.exit()
                
                # ? Read custom tests file
                with open(custom_tests_file, 'r') as f:
                    custom_tests = f.readlines()
                    
                # ? Remove all \n from custom tests and strip whitespace
                custom_tests = [test.replace('\n', '').strip() for test in custom_tests]
                    
                permutations = [parse_permutation_name(test) for test in custom_tests]
            else:
                settings = campaign['settings']
                setting_names = list(settings.keys())
                setting_values = [settings[name] for name in setting_names]
                permutations = list(itertools.product(*setting_values))
            
            total_permutations = len(permutations)
                
            if resumption_mode:
                # ? Get all permutations after the last permutation (inclusive)
                last_permutation_index = permutations.index(last_permutation)
                permutations = permutations[last_permutation_index:]
                start_index = last_permutation_index + 1
                
                # ? Calculate total number of permutations
            else:
                start_index = 1
            
            statuses = []
            retry_permutations = []
            
            write_permutations_to_file(os.path.join(campaign_folder, 'tests.txt'), permutations)
            
            for i, permutation in enumerate(permutations, start=start_index):
                start_time = time.time()
                permutation_name = generate_permutation_name(permutation)
                console.print(f"[{i}/{total_permutations}] Running Test: {permutation_name}...")
                scripts = generate_scripts(permutation)
                machines = campaign['machines']
                scripts_per_machine_list = distribute_scripts(scripts, machines)
                
                # ? Break if the last 10 tests have unreachable statuses
                # ? Get the amount of unreachable statuses in the last 10 tests
                last_10_statuses = statuses[-10:]
                unreachable_count = sum(1 for status in last_10_statuses if 'unreachable' in [machine_status['status'] for machine_status in status['machine_statuses']])
                if unreachable_count == 10:
                    console.print(f"Machines have been [bold red]UNREACHABLE[/bold red] for the last 10 tests. Diagnosing issue.", style="bold white")
                    
                    machine_response_statuses = get_machine_response_statuses(machines)
                    
                    machine_response_table = Table(title="Machine Response Statuses")
                    machine_response_table.add_column("Host", justify="center")
                    machine_response_table.add_column("IP", justify="center")
                    machine_response_table.add_column("Response (Ping/SSH)", justify="center")

                    for machine in machine_response_statuses:
                        ping_emoji = "✅" if machine['ping_response'] else "❌"
                        ssh_emoji = "✅" if machine['ssh_response'] else "❌"
                        machine_response_table.add_row(machine['name'], machine['host'], f"{ping_emoji}/{ssh_emoji}")

                    console.print(machine_response_table)
                    
                    # ? Write machine response statuses to json file.
                    machine_response_status_json = os.path.join(campaign_folder, 'machine_status_response.json')
                    with open(machine_response_status_json, 'w') as f:
                        json.dump(machine_response_statuses, f)
                    
                    # ? End the status file with ]
                    with open(statuses_file, 'a') as f:
                        f.write(']')
                    
                    # ? Move the status file to the campaign folder
                    os.rename(statuses_file, os.path.join(campaign_folder, statuses_file))
                    
                    if os.path.exists(campaign_folder + "_raw"):
                        os.rename(campaign_folder + "_raw", campaign_folder + "_raw_1")
                        new_name = campaign_folder + "_raw_2"
                    else:
                        new_name = campaign_folder + "_raw"
                    
                    # ? Rename the campaign folder to add _raw at the end
                    os.rename(campaign_folder, new_name)
                    
                    # ? Zip the campaign folder
                    zip_folder(new_name)
                    
                    sys.exit()
                
                with Manager() as manager:
                    machine_statuses = manager.list()
                    
                    machine_process_map = []
                    processes = []
                    used_colors = set()
                    for i, machine in enumerate(machines):
                        # ? Pick a random color for the machine text output
                        random_color = random.choice(COLORS)
                        if random_color in used_colors:
                            random_color = pick_contrasting_color(random_color)
                        used_colors.add(random_color)
                        script_string = scripts_per_machine_list[i]
                        duration_s = permutation[0]
                        timeout_s = duration_s + buffer_duration
                        extended_timeout_s = duration_s + (2 * buffer_duration)
                        process = Process(target=ssh_to_machine, args=(machines, machine, script_string, timeout_s, machine_statuses, permutation_name, campaign_folder, random_color))
                        machine_process_map.append((machine, process))
                        processes.append(process)
                        try:
                            process.start()
                        except Exception as e:
                            console.print(f"Caught exception from Process: {e}", style="bold red")

                    # wait for the processes to finish or timeout_s
                    start_time = time.time()
                    for process in processes:
                        while process.is_alive():
                            if time.time() - start_time > extended_timeout_s:
                                process.terminate()
                                # ? Set status to prolonged for each machine status

                                machine_from_process = [machine for machine, p in machine_process_map if p == process][0]

                                status = {
                                    "host": machine_from_process['host'],
                                    "name": machine_from_process['name'],
                                    "status": "Prolonged.",
                                    "pings": 0,
                                    "ssh_pings": 0
                                }
                                
                                machine_statuses.append(status)
                                
                                console.print(f"{machine_from_process['name']} timed out after {extended_timeout_s} seconds and was terminated", style="bold red")
                                break
                        process.join()

                    # ? Check if all machines returned no csv files and add to retry_permutations if so
                    all_no_csv_files = all("no csv files" in status['status'] for status in machine_statuses)
                    if all_no_csv_files:
                        retry_permutations.append(permutation)
                    
                    console.print("Writing test status to file...", style="bold white")
                    # Write the statuses to a file
                    with open(statuses_file, 'a') as f:
                        end_time = time.time()
                        start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
                        end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
                        result = {
                            'permutation_name': permutation_name, 
                            'machine_statuses': list(machine_statuses),
                            'start_time': start_time_str,
                            'end_time': end_time_str,
                            'duration_s': int(end_time - start_time)
                        }
                        pprint(result)
                        json.dump(result, f, indent=4)
                        statuses.append(result)
                        f.write(',\n')
                        
                console.print(f"[bold green]{permutation_name} completed in {time.time() - start_time:.2f} seconds[/bold green]")

            if len(retry_permutations) > 0:
                # console.print(f"{len(retry_permutations)} tests failed. Retrying...", style="bold red")
                # ? Check for retry_permutations and run them again
                for i, permutation in enumerate(retry_permutations):
                    start_time = time.time()
                    permutation_name = generate_permutation_name(permutation)
                    console.print(f"[{i + 1}/{len(retry_permutations)}] Retrying {permutation_name}...")
                    scripts = generate_scripts(permutation)
                    machines = campaign['machines']
                    scripts_per_machine_list = distribute_scripts(scripts, machines)
                    
                    with Manager() as manager:
                        machine_statuses = manager.list()
                        
                        processes = []
                        used_colors = set()
                        for i, machine in enumerate(machines):
                            # ? Pick a random color for the machine text output
                            random_color = random.choice(COLORS)
                            if random_color in used_colors:
                                random_color = pick_contrasting_color(random_color)
                            used_colors.add(random_color)
                            script_string = scripts_per_machine_list[i]
                            duration_s = permutation[0]
                            timeout_s = duration_s + buffer_duration
                            process = Process(target=ssh_to_machine, args=(machines, machine, script_string, duration_s, machine_statuses, permutation_name, campaign_folder, random_color))
                            processes.append(process)
                            try:
                                process.start()
                            except Exception as e:
                                console.print(f"Caught exception from Process: {e}", style="bold red")

                        start_time = time.time()
                        for process in processes:
                            while process.is_alive():
                                if time.time() - start_time > timeout_s:
                                    process.terminate()
                                    # ? Set status to prolonged for each machine status

                                    machine_from_process = [machine for machine, p in machine_process_map if p == process][0]

                                    status = {
                                        "host": machine_from_process['host'],
                                        "name": machine_from_process['name'],
                                        "status": "Prolonged process.",
                                        "pings": 0,
                                        "ssh_pings": 0
                                    }
                                    
                                    machine_statuses.append(status)
                                    
                                    console.print(f"{machine_from_process['name']} timed out after {timeout_s} seconds and was terminated", style="bold red")
                                    break
                            process.join()

                        # Write the statuses to a file
                        with open(statuses_file, 'a') as f:
                            end_time = time.time()
                            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
                            end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
                            result = {
                                'permutation_name': permutation_name, 
                                'machine_statuses': list(machine_statuses),
                                'start_time': start_time_str,
                                'end_time': end_time_str,
                                'duration_s': int(end_time - start_time)
                            }
                            json.dump(result, f, indent=4)
                            f.write(',\n')
            else:
                console.print(f"[bold green]Campaign {campaign_name} completed successfully![/bold green]")
            
            # ? End the status file with ]
            with open(statuses_file, 'a') as f:
                f.write(']')
            
            # ? Move the status file to the campaign folder
            os.rename(statuses_file, os.path.join(campaign_folder, statuses_file))
            
            if os.path.exists(campaign_folder + "_raw"):
                os.rename(campaign_folder + "_raw", campaign_folder + "_raw_1")
                new_name = campaign_folder + "_raw_2"
            else:
                new_name = campaign_folder + "_raw"
            
            # ? Rename the campaign folder to add _raw at the end
            os.rename(campaign_folder, new_name)
            
            # ? Zip the campaign folder
            zip_folder(new_name)
            
        else:
            # ? USING RANDOM COMBINATION GENERATION
            camp_index = config_data.index(campaign)
            console.print(Markdown(f"# [{camp_index + 1}/{len(config_data)}] Running Campaign: {campaign_name}"))
            
            # ? Get number of tests from config
            total_test_count = campaign['random_combination_count']
            settings = campaign['settings']
            
            for key, value in settings.items():
                if len(value) > 3:
                    raise ValueError(f"Setting {key} has more than three values: {value}")
            
            retry_permutations = []
            statuses = []
            
            permutation_history = []
            for i in range(total_test_count):
                permutation = generate_random_permutation(settings)
                while permutation in permutation_history:
                    permutation = generate_random_permutation(settings)
                permutation_history.append(permutation)
                
                start_time = time.time()
                permutation_name = generate_permutation_name(permutation)
                console.print(f"[{i + 1}/{total_test_count}] Running Test: {permutation_name}...")
                scripts = generate_scripts(permutation)
                machines = campaign['machines']
                scripts_per_machine_list = distribute_scripts(scripts, machines)
                
                # ? Break if the last 10 tests have unreachable statuses
                # ? Get the amount of unreachable statuses in the last 10 tests
                last_10_statuses = statuses[-10:]
                unreachable_count = sum(1 for status in last_10_statuses if 'unreachable' in [machine_status['status'] for machine_status in status['machine_statuses']])
                if unreachable_count == 10:
                    console.print(f"Machines have been [bold red]UNREACHABLE[/bold red] for the last 10 tests. Diagnosing issue.", style="bold white")
                    
                    machine_response_statuses = get_machine_response_statuses(machines)
                    
                    machine_response_table = Table(title="Machine Response Statuses")
                    machine_response_table.add_column("Host", justify="center")
                    machine_response_table.add_column("IP", justify="center")
                    machine_response_table.add_column("Response (Ping/SSH)", justify="center")

                    for machine in machine_response_statuses:
                        ping_emoji = "✅" if machine['ping_response'] else "❌"
                        ssh_emoji = "✅" if machine['ssh_response'] else "❌"
                        machine_response_table.add_row(machine['name'], machine['host'], f"{ping_emoji}/{ssh_emoji}")

                    console.print(machine_response_table)
                    
                    # ? Write machine response statuses to json file.
                    machine_response_status_json = os.path.join(campaign_folder, 'machine_status_response.json')
                    with open(machine_response_status_json, 'w') as f:
                        json.dump(machine_response_statuses, f)
                    
                    # ? End the status file with ]
                    with open(statuses_file, 'a') as f:
                        f.write(']')
                    
                    # ? Move the status file to the campaign folder
                    os.rename(statuses_file, os.path.join(campaign_folder, statuses_file))
                    
                    if os.path.exists(campaign_folder + "_raw"):
                        os.rename(campaign_folder + "_raw", campaign_folder + "_raw_1")
                        new_name = campaign_folder + "_raw_2"
                    else:
                        new_name = campaign_folder + "_raw"
                    
                    # ? Rename the campaign folder to add _raw at the end
                    os.rename(campaign_folder, new_name)
                    
                    # ? Zip the campaign folder
                    zip_folder(new_name)
                    
                    sys.exit()
                
                with Manager() as manager:
                    machine_statuses = manager.list()
                    
                    processes = []
                    used_colors = set()
                    for i, machine in enumerate(machines):
                        # ? Pick a random color for the machine text output
                        random_color = random.choice(COLORS)
                        if random_color in used_colors:
                            random_color = pick_contrasting_color(random_color)
                        used_colors.add(random_color)
                        script_string = scripts_per_machine_list[i]
                        duration_s = permutation[0]
                        timeout_s = duration_s + buffer_duration
                        process = Process(target=ssh_to_machine, args=(machines, machine, script_string, duration_s, machine_statuses, permutation_name, campaign_folder, random_color))
                        processes.append(process)
                        try:
                            process.start()
                        except Exception as e:
                            console.print(f"Caught exception from Process: {e}", style="bold red")

                    start_time = time.time()
                    for process in processes:
                        while process.is_alive():
                            if time.time() - start_time > timeout_s:
                                process.terminate()
                                # ? Set status to prolonged for each machine status

                                machine_from_process = [machine for machine, p in machine_process_map if p == process][0]

                                status = {
                                    "host": machine_from_process['host'],
                                    "name": machine_from_process['name'],
                                    "status": "Prolonged.",
                                    "pings": 0,
                                    "ssh_pings": 0
                                }
                                
                                machine_statuses.append(status)
                                
                                console.print(f"{machine_from_process['name']} timed out after {timeout_s} seconds and was terminated", style="bold red")
                                break
                        process.join()

                    # ? Check if all machines returned no csv files and add to retry_permutations if so
                    all_no_csv_files = all("no csv files" in status['status'] for status in machine_statuses)
                    if all_no_csv_files:
                        retry_permutations.append(permutation)

                    # Write the statuses to a file
                    with open(statuses_file, 'a') as f:
                        end_time = time.time()
                        start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
                        end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
                        result = {
                            'permutation_name': permutation_name, 
                            'machine_statuses': list(machine_statuses),
                            'start_time': start_time_str,
                            'end_time': end_time_str,
                            'duration_s': int(end_time - start_time)
                        }
                        json.dump(result, f, indent=4)
                        statuses.append(result)
                        f.write(',\n')
                        
                console.print(f"[bold green]{permutation_name} completed in {time.time() - start_time:.2f} seconds[/bold green]")
            
            if len(retry_permutations) > 0:
                # console.print(f"{len(retry_permutations)} tests failed. Retrying...", style="bold red")
                # ? Check for retry_permutations and run them again
                for i, permutation in enumerate(retry_permutations):
                    start_time = time.time()
                    permutation_name = generate_permutation_name(permutation)
                    console.print(f"[{i + 1}/{len(retry_permutations)}] Retrying {permutation_name}...")
                    scripts = generate_scripts(permutation)
                    machines = campaign['machines']
                    scripts_per_machine_list = distribute_scripts(scripts, machines)
                    
                    with Manager() as manager:
                        machine_statuses = manager.list()
                        
                        used_colors = set()
                        
                        processes = []
                        machine_colors = {}
                        for i, machine in enumerate(machines):
                            # ? Pick a random color for the machine text output
                            random_color = random.choice(COLORS)
                            if random_color in used_colors:
                                random_color = pick_contrasting_color(random_color)
                            used_colors.add(random_color)
                            
                            script_string = scripts_per_machine_list[i]
                            duration_s = campaign.get('duration_s', 60)
                            timeout_s = duration_s + buffer_duration
                            process = Process(target=ssh_to_machine, args=(machines, machine, script_string, duration_s, machine_statuses, permutation_name, campaign_folder, random_color))
                            processes.append(process)
                            try:
                                process.start()
                            except Exception as e:
                                console.print(f"Caught exception from Process: {e}", style="bold red")

                        start_time = time.time()
                        for process in processes:
                            while process.is_alive():
                                if time.time() - start_time > timeout_s:
                                    process.terminate()
                                    # ? Set status to prolonged for each machine status

                                    machine_from_process = [machine for machine, p in machine_process_map if p == process][0]

                                    status = {
                                        "host": machine_from_process['host'],
                                        "name": machine_from_process['name'],
                                        "status": "Prolonged.",
                                        "pings": 0,
                                        "ssh_pings": 0
                                    }
                                    
                                    machine_statuses.append(status)
                                    
                                    console.print(f"{machine_from_process['name']} timed out after {timeout_s} seconds and was terminated", style="bold red")
                                    break
                            process.join()

                        # Write the statuses to a file
                        with open(statuses_file, 'a') as f:
                            end_time = time.time()
                            start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
                            end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
                            result = {
                                'permutation_name': permutation_name, 
                                'machine_statuses': list(machine_statuses),
                                'start_time': start_time_str,
                                'end_time': end_time_str,
                                'duration_s': int(end_time - start_time)
                            }
                            json.dump(result, f, indent=4)
                            f.write(',\n')
            else:
                console.print(f"[bold green]Campaign {campaign_name} completed successfully![/bold green]")
            
            # ? End the status file with ]
            with open(statuses_file, 'a') as f:
                f.write(']')
            
            # ? Move the status file to the campaign folder
            os.rename(statuses_file, os.path.join(campaign_folder, statuses_file))
            
            if os.path.exists(campaign_folder + "_raw"):
                os.rename(campaign_folder + "_raw", campaign_folder + "_raw_1")
                new_name = campaign_folder + "_raw_2"
            else:
                new_name = campaign_folder + "_raw"
            
            # ? Rename the campaign folder to add _raw at the end
            os.rename(campaign_folder, new_name)
            
            # ? Zip the campaign folder
            zip_folder(new_name)
            

if __name__ == '__main__':
    main()