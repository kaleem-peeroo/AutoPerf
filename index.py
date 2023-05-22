from functions import *

args = sys.argv[1:]

sys.stdout = Tee(sys.stdout, open('output.txt', 'w'))

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

camp_combinations_table = Table(title="Combinations Count Per Campaign", show_lines=True)
camp_combinations_table.add_column("#")
camp_combinations_table.add_column("Campaign")
camp_combinations_table.add_column("Combination Count")
for comb in combinations:
    camp_combinations_table.add_row(str(combinations.index(comb) + 1), comb['name'], str(len(comb['combinations'])))

console.print(camp_combinations_table)

# ? Print out the test names per campaign.
for comb in combinations:
    filename = comb["name"].lower().replace(" ", "_") + "_test_combinations.txt"
    
    test_titles = []
    
    for combination in comb["combinations"]:
        test_title = get_test_title_from_combination(combination)
        test_titles.append(test_title)
        
    with open(filename, "w") as f:
        f.write("\n".join(test_titles))
        
    console.print(f"Test titles for {comb['name']} written to [bold blue]{filename}[/bold blue].")
    
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

            if "scripts" not in machine:
                machine["scripts"] = f"source ~/.bashrc; {scripts};" if len(scripts) > 0 else f"source ~/.bashrc;"
            else:
                old_scripts = machine["scripts"]
                machine["scripts"] = f"{old_scripts}; {scripts};" if len(scripts) > 0 else f"{old_scripts};"

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
            
            if "scripts" not in machine:
                machine["scripts"] = f"source ~/.bashrc; {scripts};" if len(scripts) > 0 else f"source ~/.bashrc;"
            else:
                old_scripts = machine["scripts"]
                machine["scripts"] = f"{old_scripts}; {scripts};" if len(scripts) > 0 else f"{old_scripts};"

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
    console.print(f"\tConfig: {args[0]}")
    
    # ? Make a folder for the campaign.
    camp_dir = create_dir(camp_name.replace(" ", "_"))
    
    # ? Move the test_titles file to the new camp_dir.
    shutil.copy(camp_name.lower().replace(" ", "_") + "_test_combinations.txt", camp_dir)

    # ? Track test statuses during run-time.
    test_statuses = []

    # ? Make progress.json for the campaign.
    log_debug(f"Making progress.json for {camp_name}...")
    progress_json = os.path.join(camp_dir, 'progress.json')
    with open(progress_json, 'w') as f:
        json.dump(test_statuses, f)
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
        
        console.print(f"\t[{format_now()}] Expected Duration (s): {expected_duration_sec}", style="bold white")
        console.print(f"\t[{format_now()}] Expected End Date: {add_seconds_to_now(expected_duration_sec)}", style="bold white")
        
        console.print(f"\n\t[{format_now()}] Buffer Duration (s): {int(expected_duration_sec * buffer_multiple)}", style="bold white")
        console.print(f"\t[{format_now()}] Expected Buffer End Date: {add_seconds_to_now(expected_duration_sec * buffer_multiple)}", style="bold white")

        if expected_duration_sec is None:
            console.print(f"[{format_now}] {ERROR} Error calculating expected time duration in seconds for\n\t{test_title}.", style="bold white")
            continue

        for machine in test["machines"]:
            # ? Replace "; source ~/.bashrc;" with " & " so that it executes the command in parallel instead of sequentially.
            machine['scripts'] = machine['scripts'].replace("; source ~/.bashrc;", " & ")
            machine["scripts"] = machine["scripts"].rstrip()
            
            # ? Find and replace the & at the end of the script with a ; or the script will hang forever because of that last &
            # ? e.g. "...pub.csv &" => "...pub.csv ;"
            if ";" not in machine["scripts"][-3:] and "&" in machine["scripts"][-3:]:
                replaced_string = ";".join( machine["scripts"].rsplit("&", 1) )
                machine["scripts"] = replaced_string
        
        # ? Write test config to file.
        with open(os.path.join(test_dir, 'config.json'), 'w') as f:
            json.dump(test, f, indent=4)
        log_debug(f"Test configuration written to {os.path.join(test_dir, 'config.json')}.")

        if not TEST_MODE:
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
        else:
            # ? Test Mode: Pretend to run the test. Randomly fail or succeed.
            console.print(f"Pretending to run {test_title}.", style="bold white")
            if random.random() < TEST_MODE_FAIL_CHANCE:
                console.print(f"Fake {test_title} failed.", style="bold red")
                test_end_status = "prolonged"
            else:
                console.print(f"Fake {test_title} succeeded.", style="bold green")
                test_end_status = "punctual"
                    
        # ? Scripts finished running at this point.
        end_time = time.time()
        
        # ? Update test status.
        test_statuses.append({
            "test_title": test_title,
            "end_time_raw": end_time,
            "start_time_raw": start_time,
            "start_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
            "end_time": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)),
            "status": test_end_status
        })
        
        # ? Order test status by end time.
        test_statuses = sorted(test_statuses, key=lambda k: k['end_time'])
        
        # ? Record test start and end time.
        update_progress(progress_json, test_title, start_time, end_time, test_end_status)

        # ? Analyse statuses for prolonged tests.
        if has_consecutive_prolonged_tests(test_statuses):
            console.print(f"The last 5 tests have been consecutively prolonged. Checking machines for response and then ending the campaign.", style="bold red")
            
            # ? Check which machines are unresponsive.
            machine_response_statuses = get_machine_response_statuses(test['machines'])
            
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
            machine_response_status_json = os.path.join(camp_dir, 'machine_status_response.json')
            with open(machine_response_status_json, 'w') as f:
                json.dump(test_statuses, f)
                
            break
        
        output_test_progress(progress_json)
        
    # ? Move the output file to the campaign folder.
    with open("output.txt", "w") as f:
        f.write(console.export_text())
    
    # ? Get latest txt file.
    latest_txt = get_latest_txt_file("./")
    # ? Move txt file to campaign folder.
    shutil.copy(latest_txt, camp_dir)
    # ? Rename the folder to <test_name>_raw
    os.rename(camp_dir, camp_dir + "_raw")
    camp_dir = camp_dir + "_raw"
    # ? Zip the campaign folder.
    zip_folder(camp_dir)