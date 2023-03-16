from functions import *

args = sys.argv[1:]

# ? Validate args
log_debug("Validating args...")
validate_args(args)
log_debug("args validated.")

# ? Read config file.
log_debug(f"Reading config: {args[0]}...")
config = read_config(args[0])
log_debug("Config read.")

# ? Calculate total number of combinations for each campaign.
console.print(f"Here are the total number of combinations for each campaign:", style="bold white")
for camp in config["campaigns"]:
    name = camp['name']
    comb_count = get_combinations_count_from_settings(camp['settings'])
    console.print(f"\t[bold blue]{name}[/bold blue]: {comb_count} combinations.")

# ? Ask user for combination modification.
if Confirm.ask("Would you like to remove some of the combinations?", default=False):
    file_combs_dir = write_combinations_to_file(config)
    
    console.print(f"\nThe combinations of each campaign have been written to /{file_combs_dir}. ", style="bold white")
    console.print(f"Go ahead and remove the tests you don't want and then come back here to continue.\n", style="bold green")

    is_ready = Confirm.ask("Are you ready?", default=True)
    while not is_ready:
        is_ready = Confirm.ask("Are you ready?", default=True)

    combinations = get_combinations_from_file(file_combs_dir, config)

    # ? Output number of combinations for confirmation.
    console.print(f"Here are the [bold blue]updated[/bold blue] total number of combinations for each campaign:", style="bold white")
    for campaign in combinations:
        camp_name = campaign['name']
        camp_combinations = campaign['combinations']
        camp_comb_count = len(camp_combinations)
        
        console.print(f"\t[bold blue]{camp_name}[/bold blue]: {camp_comb_count} combinations.", style="bold white")

else:
    combinations = get_combinations_from_config(config)


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
    machine_count = len(camp_config['machines'])

    test_scripts = []

    log_debug(f"Generating scripts for {len(test_combs)} tests in {camp_name}.")

    for test_comb in test_combs:
        scripts = generate_scripts(test_comb)
        pub_scripts, sub_scripts = allocate_scripts_per_machine(scripts, machine_count)

        perftest = camp_config['machines'][0]["perftest"]
        
        machine_scripts = []

        for machine, pub_script, sub_script in zip(camp_config['machines'], pub_scripts, sub_scripts):
            machine_pub_scripts = [f"{perftest} {script}" for script in pub_script]
            machine_sub_scripts = [f"{perftest} {script}" for script in sub_script]

            machine_pub_scripts = " & ".join(machine_pub_scripts) if len(machine_pub_scripts) > 1 else machine_pub_scripts[0]
            machine_sub_scripts = " & ".join(machine_sub_scripts) if len(machine_sub_scripts) > 1 else machine_sub_scripts[0]

            # TODO: Turn the two copies of code below into a function.

            # ? Add the home_dir path to the start of the outputFile path
            machine_pub_scripts_list = machine_pub_scripts.split()
            for i in range(len(machine_pub_scripts_list)):
                if machine_pub_scripts_list[i] == '-outputFile':
                    if i + 1 < len(machine_pub_scripts_list):
                        output_file = machine_pub_scripts_list[i + 1]
                        home_dir = machine['home_dir']
                        new_output_file = os.path.join(home_dir, output_file)
                        machine_pub_scripts_list[i + 1] = new_output_file

            machine_pub_scripts = " ".join(machine_pub_scripts_list)

            # ? Add the home_dir path to the start of the outputFile path
            machine_sub_scripts_list = machine_sub_scripts.split()
            for i in range(len(machine_sub_scripts_list)):
                if machine_sub_scripts_list[i] == '-outputFile':
                    if i + 1 < len(machine_sub_scripts_list):
                        output_file = machine_sub_scripts_list[i + 1]
                        home_dir = machine['home_dir']
                        new_output_file = os.path.join(home_dir, output_file)
                        machine_sub_scripts_list[i + 1] = new_output_file

            machine_sub_scripts = " ".join(machine_sub_scripts_list)

            if len(machine_pub_scripts) > 0 and len(machine_sub_scripts) > 0:
                machine_script = machine_pub_scripts + " & " + machine_sub_scripts
            elif len(machine_pub_scripts) > 0 and len(machine_sub_scripts) == 0:
                machine_script = machine_pub_scripts
            elif len(machine_pub_scripts) == 0 and len(machine_sub_scripts) > 0:
                machine_script = machine_sub_scripts
            else:
                machine_script = None

            machine.update({"scripts": f"source ~/.bashrc; {machine_script}"})
            machine_scripts.append(machine)

        # ? Number of machines = number of machine scripts
        assert(len(camp_config['machines']) == len(machine_scripts))

        test_scripts.append({
            "combination": test_comb,
            "machines": machine_scripts
        })

    # ? Number of tests = number of scripts per test
    assert(len(test_combs) == len(test_scripts))

    campaign_scripts.append({
        "name": camp_name,
        "tests": test_scripts
    })

    log_debug(f"Scripts generated for {camp_name}.")

# ? Number of campaigns with scripts generated = number of campaigns
assert(len(campaign_scripts) == len(combinations))
        
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
for campaign in campaign_scripts:
    camp_name = campaign['name']
    
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
        continue

    for test in tests:
        start_time = time.time()

        # ? Make a folder for the test
        test_title = get_test_title_from_combination(test['combination'])
        test_dir = create_dir( os.path.join(camp_dir, test_title) )
        log_debug(f"Made testdir: {test_dir}.")
        
        # ? Get expected test duration in seconds.
        expected_duration_sec = get_duration_from_test_name(test_title)
        log_debug(f"Expected Duration (s) for {test_title}: {expected_duration_sec} seconds.")
        if expected_duration_sec is None:
            console.print(f"{ERROR} Error calculating expected time duration in seconds for\n\t{test_title}.", style="bold white")
            continue

        with console.status(f"[{tests.index(test) + 1}/{len(tests)}] Running test: {test_title}..."):
            console.print(f"[{tests.index(test) + 1}/{len(tests)}] Running test: {test_title}.")

            # ? Write test config to file.
            with open(os.path.join(test_dir, 'config.json'), 'w') as f:
                json.dump(test, f, indent=4)
            log_debug(f"Test configuration written to {os.path.join(test_dir, 'config.json')}.")

            # ? Create threads for each machine.
            machine_threads = []
            for machine in test['machines']:
                # machine_thread = Thread(target=machine_thread_func, args=(machine, test_dir))
                machine_thread = multiprocessing.Process(target=machine_thread_func, args=(machine, test_dir))
                machine_threads.append(machine_thread)
                machine_thread.start()

            for machine_thread in machine_threads:
                machine_thread.join(timeout=expected_duration_sec * 1.5)
                
                # ? If thread is still alive kill it.
                if machine_thread.is_alive():
                    machine_thread.terminate()
                    console.print(f"{ERROR} {test_title} timed out after a duration of {expected_duration_sec * 1.5} seconds.", style="bold white")

        # ? Scripts finished running at this point.
        
        end_time = time.time()
        
        # ? Record test start and end time.
        update_progress(progress_json, test_title, start_time, end_time)