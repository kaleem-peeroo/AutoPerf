from functions import *

args = sys.argv[1:]

# ? Validate args
log_debug("Validating args...")
validate_args(args)
log_debug("args validated.")

console.print(f"Running PTST with the following arguments:", style="bold white")
console.print(f"\tConfig: {args[0]}")
console.print(f"\tBuffer Multiple: {args[1]}\n")

# ? Read config file.
log_debug(f"Reading config: {args[0]}...")
config = read_config(args[0])
log_debug("Config read.")

# ? Read in buffer_multiple:
buffer_multiple = float(args[1])

combinations = get_combinations_from_config(config)

# # ? Calculate total number of combinations for each campaign.
# console.print(f"Here are the total number of combinations for each campaign:", style="bold white")
# for camp in config["campaigns"]:
#     name = camp['name']
#     comb_count = get_combinations_count_from_settings(camp['settings'])
#     console.print(f"\t[bold blue]{name}[/bold blue]: {comb_count} combinations.")

camp_combinations_table = Table(title="Combinations Count Per Campaign", show_lines=True)
camp_combinations_table.add_column("#")
camp_combinations_table.add_column("Campaign")
camp_combinations_table.add_column("Combination Count")
for comb in combinations:
    camp_combinations_table.add_row(str(combinations.index(comb) + 1), comb['name'], str(len(comb['combinations'])))

console.print(camp_combinations_table)

"""
combinations = [{
    'name': 'camp_name',
    'combinations': [
        {
            'durability': ...,
            'multicast': ...,
            ...
        }
    ]
}]
"""
campaign_scripts = []
for camp_comb in combinations:
    
    camp_name = camp_comb['name']
    test_combs = camp_comb['combinations']

    camp_config = [_ for _ in config['campaigns'] if _['name'] == camp_name][0]
    
    test_scripts = []

    log_debug(f"Generating scripts for {len(test_combs)} tests in {camp_name}.")

    for test_comb in test_combs:
        machines_conf = camp_config['machines']
        
        machine_scripts = []
        scripts = generate_scripts(test_comb)
        
        if not validate_scripts(test_comb, scripts):
            console.print(f"[{format_now}] {ERROR} Error when validating scripts. Scripts are NOT valid.", style="bold red")
            sys.exit()

        participant_allocations = [machine['participant_allocation'] for machine in machines_conf]
        
        pub_machines = [_ for _ in machines_conf if "pub" in _['participant_allocation'] or "all" in _['participant_allocation']]
        sub_machines = [_ for _ in machines_conf if "sub" in _['participant_allocation'] or "all" in _['participant_allocation']]
        
        pub_machines_count = len(pub_machines)
        sub_machines_count = len(sub_machines)
        
        pub_scripts = [_ for _ in scripts if '-pub' in _]
        sub_scripts = [_ for _ in scripts if '-sub' in _]
        
        balanced_pub_scripts = share(pub_scripts, pub_machines_count) if pub_machines_count > 1 else [pub_scripts]
        balanced_sub_scripts = share(sub_scripts, sub_machines_count) if sub_machines_count > 1 else [sub_scripts]
            
        loaded_machines_conf = []
                
        for i in range(pub_machines_count):
            machine = dict(pub_machines[i])
            scripts = balanced_pub_scripts[i]
            
            if type(scripts) is not list:
                scripts = [scripts]
            
            perftest = machine["perftest"]
            scripts = [f"{perftest} {script}" for script in scripts]
            
            updated_scripts = []
            
            # ? Add the home_dir path to the outputFile path.
            for script in scripts:
                script_items = script.split()
                for j in range(len(script_items)):
                    if script_items[j] == "-outputFile":
                        # ? Avoid accessing outside of list.
                        if j + 1 < len(script_items):
                            output_dir = script_items[j + 1]
                            home_dir = machine['home_dir']
                            output_dir = os.path.join(home_dir, output_dir)
                            script_items[j + 1] = output_dir
                script = " ".join(script_items)
                updated_scripts.append(script)                
                        
            scripts = updated_scripts
            
            scripts = " & ".join(scripts)

            try:
                machine["scripts"] = {machine["scripts"]} + f" & {scripts}"
            except KeyError as e:
                machine["scripts"] = f"source ~/.bashrc; {scripts}"

            loaded_machines_conf.append(machine)

        for i in range(sub_machines_count):
            machine = dict(sub_machines[i])
            scripts = balanced_sub_scripts[i]
            
            perftest = machine["perftest"]
            scripts = [f"{perftest} {script}" for script in scripts]
            
            if type(scripts) is not list:
                scripts = [scripts]
            
            updated_scripts = []
            
            # ? Add the home_dir path to the outputFile path.
            for script in scripts:
                script_items = script.split()
                for j in range(len(script_items)):
                    if script_items[j] == "-outputFile":
                        # ? Avoid accessing outside of list.
                        if j + 1 < len(script_items):
                            output_dir = script_items[j + 1]
                            home_dir = machine['home_dir']
                            output_dir = os.path.join(home_dir, output_dir)
                            script_items[j + 1] = output_dir
                script = " ".join(script_items)
                updated_scripts.append(script)                
                        
            scripts = updated_scripts
            
            scripts = " & ".join(scripts)
            
            try:
                old_machine_scripts = machine["scripts"]
            
                machine["scripts"] = f"{old_machine_scripts} & {scripts}"
            except KeyError as e:
                machine["scripts"] = f"source ~/.bashrc; {scripts}"
            
            loaded_machines_conf.append(machine)
        
        # ? Combine any repeated machine config objects.
        merged_machines_conf = []
        for item in loaded_machines_conf:
            found = False
            for merged_item in merged_machines_conf:
                if merged_item['name'] == item['name']:
                    found = True
                    merged_item['scripts'] += ' ' + item['scripts']
                    break
            if not found:
                merged_machines_conf.append(item)
        
        loaded_machines_conf = merged_machines_conf

        test_scripts.append({
            "combination": test_comb,
            "machines": loaded_machines_conf
        })
        
    campaign_scripts.append({
        "name": camp_name,
        "tests": test_scripts
    })


    log_debug(f"Scripts generated for {camp_name}.")
    
# ? At this point, every campaign contains a test. Every test contains its combination and the machines involved. Every machine contains the relevant machine information as well as its scripts to run.
"""
campaign_scripts = [{
    'name': 'camp_name',
    'tests': [
        {
            'combination' : {'duration_s': 600, ...},
            'machines': [
                {'name': 'p1', 'host': '...', ..., 'scripts': '...'},
                {'name': 'p2', 'host': '...', ..., 'scripts': '...'}
            ]
        },
        {
            'combination' : {'duration_s': 600, ...},
            'machines': [
                {'name': 'p1', 'host': '...', ..., 'scripts': '...'},
                {'name': 'p2', 'host': '...', ..., 'scripts': '...'}
            ]
        },
        ...
    ]
}, ...]
"""
duration_s_per_camp = calculate_total_duration(campaign_scripts)
total_duration_s = sum(item['duration_s'] for item in duration_s_per_camp)
current_time = datetime.now()
campaign_expected_end_time = current_time + timedelta(seconds=total_duration_s)
campaign_expected_end_time = campaign_expected_end_time.strftime("%Y-%m-%d %H:%M:%S")

total_duration_stats_table = Table(show_lines=True)
total_duration_stats_table.add_column("Campaign")
total_duration_stats_table.add_column("Expected Duration")
total_duration_stats_table.add_column("Expected End Date")

for item in duration_s_per_camp:
    end_date = current_time + timedelta(seconds = item['duration_s'])
    end_date = end_date.strftime("%Y-%m-%d %H:%M:%S")
    total_duration_stats_table.add_row(item['camp'], f"{convert_seconds(item['duration_s'])}", f"{end_date}")

total_duration_stats_table.add_row("Total", f"{convert_seconds(total_duration_s)}", f"{campaign_expected_end_time}")

console.print(total_duration_stats_table)

for campaign in campaign_scripts:
    camp_name = campaign['name']
    
    console.print(f"[{format_now()}] [{campaign_scripts.index(campaign) + 1}/{len(campaign_scripts)}] Running Campaign: {camp_name}", style="bold white")
    
    # ? Make a folder for the campaign.
    camp_dir = create_dir(camp_name.replace(" ", "_"))

    # ? Make progress.json for the campaign.
    log_debug(f"Making progress.json for {camp_name}...")
    progress_json = os.path.join(camp_dir, 'progress.json')
    with open(progress_json, 'w') as f:
        json.dump([], f)
    log_debug(f"progress.json made for {camp_name}.")

    tests = campaign['tests']

    # ? Check for no tests.
    if len(tests) == 0:
        console.print(f"{WARNING} No tests found in {camp_name}.\n", style="bold white")
        continue    # ? Go to next loop i.e next campaign in campaign_scripts

    for test in tests:
        start_time = time.time()
        test_end_status = "punctual"
        
        # ? Make a folder for the test
        test_title = get_test_title_from_combination(test['combination'])
        test_dir = create_dir( os.path.join(camp_dir, test_title) )
        log_debug(f"Made testdir: {test_dir}.")

        console.print(f"\n[{format_now()}] [{tests.index(test) + 1}/{len(tests)}] Running Test: {test_title}")

        # ? Get expected test duration in seconds.
        expected_duration_sec = get_duration_from_test_name(test_title)
        
        console.print(f"\t[{format_now()}] Expected Duration (s) for {test_title}: {expected_duration_sec} seconds.", style="bold white")
        console.print(f"\t[{format_now()}] Expected End Date: {add_seconds_to_now(expected_duration_sec)}", style="bold white")
        
        console.print(f"\n\t[{format_now()}] Buffer Duration (s) for {test_title}: {int(expected_duration_sec * buffer_multiple)} seconds.", style="bold white")
        console.print(f"\t[{format_now()}] Expected Buffer End Date: {add_seconds_to_now(expected_duration_sec * buffer_multiple)}", style="bold white")

        if expected_duration_sec is None:
            console.print(f"[{format_now}] {ERROR} Error calculating expected time duration in seconds for\n\t{test_title}.", style="bold white")
            continue

        # ? Write test config to file.
        with open(os.path.join(test_dir, 'config.json'), 'w') as f:
            json.dump(test, f, indent=4)
        log_debug(f"Test configuration written to {os.path.join(test_dir, 'config.json')}.")

        # ? Create processes for each machine.
        machine_processes = []
        machine_processes_machines = []
        for machine in test['machines']:
            machine_process = multiprocessing.Process(target=machine_process_func, args=(machine, test_dir, buffer_multiple))
            machine_processes.append(machine_process)
            machine_processes_machines.append(machine)
            machine_process.start()

        for machine_process in machine_processes:
            i = machine_processes.index(machine_process)
            machine_name = machine_processes_machines[i]['name']
            machine_process.join(timeout=int(expected_duration_sec * buffer_multiple))
            
            # ? If process is still alive kill all processes.
            if machine_process.is_alive():
                
                for machine_process_j in machine_processes:
                    machine_process_j.terminate()
                
                console.print(f"[{format_now()}] {ERROR} {machine_name} {test_title} timed out after a duration of {int(expected_duration_sec * buffer_multiple)} seconds.", style="bold white")
                test_end_status = "prolonged"
                    
        # ? Scripts finished running at this point.
        
        end_time = time.time()
        
        # ? Record test start and end time.
        update_progress(progress_json, test_title, start_time, end_time, test_end_status)

        output_test_progress(progress_json)