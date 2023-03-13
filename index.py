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
campaign_scripts = []
for item in combinations:
    machine_scripts = []
    
    campaign = item['name']
    
    camp_config = [item for item in config['campaigns'] if item['name'] == campaign][0]
    
    combs = item['combinations']
    
    scripts = [generate_scripts(comb) for comb in combs]
    
    pub_scripts, sub_scripts = allocate_scripts_per_machine(scripts, len(camp_config['machines']))

    machines = camp_config['machines']

    for i in range(len(machines)):            
        perftest = machines[i]["perftest"]
        
        if len(pub_scripts) > 1:
            machine_pub_scripts = [perftest + " " + x for x in pub_scripts[i]]
        else:
            machine_pub_scripts = pub_scripts
        
        if len(sub_scripts) > 1:
            machine_sub_scripts = [perftest + " " + x for x in sub_scripts[i]]
        else:
            machine_sub_scripts = sub_scripts
        
        machine_script = {
            "name": machines[i]["name"],
            "host": machines[i]["host"],
            "ssh_key": machines[i]["ssh_key"],
            "username": machines[i]["username"],
            "perftest": machines[i]["perftest"],
            "home_dir": machines[i]["home_dir"],
            "pub_scripts": machine_pub_scripts,
            "sub_scripts": machine_sub_scripts
        }
        
        
        machine_scripts.append(machine_script)

    campaign_scripts.append({
        'name': campaign,
        'combinations': combs,
        'scripts': scripts,
        'pub_scripts': pub_scripts,
        'sub_scripts': sub_scripts,
        'machines': machine_scripts
    })

