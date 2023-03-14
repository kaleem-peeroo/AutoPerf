from functions import *

args = sys.argv[1:]

# ? Validate args
console.print(f"{DEBUG}Validating args...", style="bold white") if DEBUG_MODE else None
validate_args(args)
console.print(f"{DEBUG}args validated.", style="bold green") if DEBUG_MODE else None

# ? Read config file.
console.print(f"{DEBUG}Reading config: {args[0]}...", style="bold white") if DEBUG_MODE else None
config = read_config(args[0])
console.print(f"{DEBUG}Config read.", style="bold green") if DEBUG_MODE else None

# TODO: Calculate total number of combinations for each campaign.
console.print(f"Here are the total number of combinations for each campaign:", style="bold white")
for camp in config["campaigns"]:
    name = camp['name']
    comb_count = get_combinations_count_from_settings(camp['settings'])
    console.print(f"\t[bold blue]{name}[/bold blue]: {comb_count} combinations.")

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


# TODO: Generate scripts for each campaign's combinations.
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

            if len(machine_pub_scripts) > 0 and len(machine_sub_scripts) > 0:
                machine_script = machine_pub_scripts + " & " + machine_sub_scripts
            elif len(machine_pub_scripts) > 0 and len(machine_sub_scripts) == 0:
                machine_script = machine_pub_scripts
            elif len(machine_pub_scripts) == 0 and len(machine_sub_scripts) > 0:
                machine_script = machine_sub_scripts
            else:
                machine_script = None

            machine.update({"scripts": machine_script})
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
"""
TODO:
For each campaign:
    For each test:
        - Make a folder of the test results - generate the name using the combination (also check if name already exists and add number at the end).
        - Write the test info (combination, machine scripts) to a .json file.
        - Create threads for each machine.
            - Check that the machine is online.
            - Check for, download, and then delete existing csv files.
            - Restart the machine.
            - Check its online.
            - Start the logging.
            - Run the scripts.
            - Wait for scripts to finish.
            - Write the stderr to a file (if it has content).
            - Check that all .csv files were generated and dowload them.
                - If some are missing - try the test again (up to 3 times before moving on to the next test).
            - Download the system logs.
"""