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
from multiprocessing import Process, Manager

console = Console()

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

def ssh_to_machine(machines, machine, script_string, timeout, machine_statuses, test_name, campaign_folder, max_retries=10):
    status = {
        "host": machine['host'],
        "name": machine['name'],
        "status": "unknown",
        "pings": 0,
    }    
    
    machine_name = machine['name']
    test_folder = os.path.join(campaign_folder, test_name)
    
    for i in range(max_retries):
        console.print(f"Pinging {machine_name} (attempt {i+1}/{max_retries})...", style="bold white")
        if ping_machine(machine):
            status['pings'] = i + 1
            break
        if i == max_retries - 1:
            status['status'] = "unreachable"
            return None, None
        time.sleep(1)
    
    # ? Ping other machines to make sure they are good to go.
    other_machines = [m for m in machines if m['name'] != machine_name]
    for other_machine in other_machines:
        other_machine_name = other_machine['name']
        for i in range(max_retries):
            console.print(f"Pinging {other_machine_name} from {machine_name} (attempt {i+1}/{max_retries})...", style="bold white")
            if ping_machine(other_machine):
                break
            if i == max_retries - 1:
                status['status'] = f"{other_machine_name} unreachable from {machine_name}"
                return None, None
            time.sleep(1)
    
    console.print(f"{machine_name}: Running scripts...", style="bold white")
    ssh_command = f"ssh {machine['username']}@{machine['host']} '{script_string}'"
    process = subprocess.Popen(ssh_command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    start_time = time.time()
    while True:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            status['status'] = "punctual"
            break
        elif time.time() - start_time > timeout:
            process.kill()
            stdout, stderr = process.communicate()
            status['status'] = "prolonged"
            break
        time.sleep(1)

    if status['status'] == "punctual":
        remote_files_command = f"ssh {machine['username']}@{machine['host']} 'ls *.csv'"
        remote_files_process = subprocess.run(remote_files_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        remote_files = remote_files_process.stdout.decode().split()
        
        if len(remote_files) == 0:
            status['status'] = "no csv files"
            console.print(f"No csv files found for {test_name} on {machine_name}.", style="bold red")
        else:
            os.makedirs(test_folder, exist_ok=True)
            
            console.print(f"{machine_name}: Downloading csv files...", style="bold white")
            for remote_file in remote_files:
                remote_path = remote_file
                local_path = os.path.join(test_folder, f"{remote_file}")
                scp_command = f"scp {machine['username']}@{machine['host']}:{remote_path} {local_path}"
                with open(os.devnull, 'w') as devnull:
                    subprocess.run(scp_command, shell=True, stdout=devnull, stderr=devnull)
            
            console.print(f"{machine_name}: Deleting csv files...", style="bold white")
            # ? Delete all csv files on remote machine
            ssh_command = f"ssh {machine['username']}@{machine['host']} 'rm *.csv'"
            with open(os.devnull, 'w') as devnull:
                subprocess.run(ssh_command, shell=True, stdout=devnull)

    console.print(f"{machine_name} Restarting machine...", style="bold white")
    # ? Restart the machine.
    ssh_command = f"ssh {machine['username']}@{machine['host']} 'sudo reboot'"
    with open(os.devnull, 'w') as devnull:
        subprocess.run(ssh_command, shell=True, stdout=devnull, stderr=devnull)
        
    # ? Wait some time for restart to happen.
    time.sleep(5)

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
    # Check if config_data is a list with a single dictionary element
    if not isinstance(config_data, list) or len(config_data) != 1 or not isinstance(config_data[0], dict):
        console.print("Error: config file must contain a list with a single dictionary element.")
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
        campaign_name = campaign['name']
        campaign_folder = campaign_name.lower().replace(' ', '_')
        os.makedirs(campaign_folder, exist_ok=True)

        statuses_file = campaign_name.lower().replace(" ", "_") + "_statuses.json"
        
        # ? Start status file with [
        with open(statuses_file, 'w') as f:
            f.write('[')
        
        # Generate all possible permutations of settings values if random combination generation is disabled
        if not campaign.get('random_combination_generation', False):
            permutations_list = []
            statuses = []
            
            settings = campaign['settings']
            setting_names = list(settings.keys())
            setting_values = [settings[name] for name in setting_names]
            permutations = list(itertools.product(*setting_values))
            
            for i, permutation in enumerate(permutations):
                start_time = time.time()
                permutation_name = generate_permutation_name(permutation)
                console.print(f"[{i + 1}/{len(permutations)}] Running {permutation_name}...")
                permutations_list.append(permutation)
                scripts = generate_scripts(permutation)
                machines = campaign['machines']
                scripts_per_machine_list = distribute_scripts(scripts, machines)
                
                with Manager() as manager:
                    machine_statuses = manager.list()
                    
                    processes = []
                    for i, machine in enumerate(machines):
                        script_string = scripts_per_machine_list[i]
                        duration_s = campaign.get('duration_s', 60)
                        timeout = duration_s + buffer_duration
                        process = Process(target=ssh_to_machine, args=(machines, machine, script_string, timeout, machine_statuses, permutation_name, campaign_folder))
                        processes.append(process)
                        try:
                            process.start()
                        except Exception as e:
                            console.print(f"Caught exception from Process: {e}", style="bold red")

                    for process in processes:
                        try:
                            process.join()
                        except Exception as e:
                            console.print(f"Caught exception from Process: {e}", style="bold red")

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
                        
                console.print(f"[bold green]{permutation_name} completed in {time.time() - start_time:.2f} seconds[/bold green]")

            console.print(f"[bold green]Campaign {campaign_name} completed successfully![/bold green]")
            
            # ? End the status file with ]
            with open(statuses_file, 'a') as f:
                f.write(']')
            
            # ? Update the status file and remove the last ,
            with open(statuses_file, 'r') as f:
                data = f.read()
            data = data.rstrip(',')
            with open(statuses_file, 'w') as f:
                f.write(data)
                            
            # ? Move the status file to the campaign folder
            os.rename(statuses_file, os.path.join(campaign_folder, statuses_file))
            
            # ? Rename the campaign folder to add _raw at the end
            os.rename(campaign_folder, campaign_folder + '_raw')
            
            # ? Zip the campaign folder
            zip_folder(campaign_folder)
            

if __name__ == '__main__':
    main()