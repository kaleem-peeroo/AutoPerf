import itertools
import argparse
import os
import json
import sys

from pprint import pprint
from rich.console import Console

console = Console()

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
    duration_s = f"{permutation[0]}S"
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

        # Generate all possible permutations of settings values if random combination generation is disabled
        if not campaign.get('random_combination_generation', False):
            permutations_list = []  # Create an empty list to store all permutations
            
            settings = campaign['settings']
            setting_names = list(settings.keys())
            setting_values = [settings[name] for name in setting_names]
            permutations = list(itertools.product(*setting_values))
            
            for permutation in permutations:
                permutation_name = generate_permutation_name(permutation)
                permutations_list.append(permutation)
                scripts = generate_scripts(permutation)
                machines = campaign['machines']
                scripts_per_machine_list = distribute_scripts(scripts, machines)
                
                for i, machine in enumerate(machines):
                    print(f"Machine {i+1} scripts:")
                    print(scripts_per_machine_list[i])
                    print()
            

if __name__ == '__main__':
    main()