from utility_functions import *

def validate_args(args):
    # ? No args given.
    if len(args) == 0:
        console.print(f"No config file given.", style="bold red")
        console.print(f"Config file expected as:\n\tpython index.py <config_file>", style="bold green")
        sys.exit()

    # ? Validate config file
    config_path = args[0]
    if not ( os.path.isfile(config_path) and os.access(config_path, os.R_OK) ):
        console.print(f"Can't access {config_path}. Check it exists and is accessible and try again.", style="bold red")
        sys.exit()

def read_config(confpath):
    try:
        with open(confpath) as f:
            config = json.load(f)

        return config
    except Exception as e:
        console.print(f"Error when reading config: \n\t{e}", style="bold red")
        sys.exit()

def get_combinations_count_from_settings(settings):
    product = 1
    
    for key in settings:
        product *= len(settings[key])
    
    return product

def write_combinations_to_file(config):
    dir_name = create_dir("test_combinations")
    
    for campaign in config['campaigns']:
        comb_filename = campaign['name'].replace(" ", '_')
        comb_filename = os.path.join(dir_name, comb_filename + ".txt")
        combinations = get_combinations(campaign['settings'])
        comb_titles = []

        for combination in combinations:
            comb_titles.append(get_test_title_from_combination(combination))

        with open(comb_filename, "w") as f:
            f.writelines(f"{title}\n" for title in comb_titles)

        console.print(f"{DEBUG} Written test combinations to {comb_filename}.", style="bold green") if DEBUG_MODE else None

    return dir_name

def get_combinations_from_file(dirpath, config):
    comb_files = os.listdir(dirpath)
    comb_files = [os.path.join(dirpath, file) for file in comb_files]

    combs = []
    for file in comb_files:
        camp_name = os.path.basename(file).replace(".json", "").replace(".txt", "").replace("_", " ")
        
        with open(file, 'r') as f:
            titles = f.readlines()

        combinations = [get_combination_from_title(title) for title in titles]

        combs.append({
            "name": camp_name,
            "combinations": combinations
        })

    return combs

def get_combinations_from_config(config):
    combs = []

    for campaign in config['campaigns']:
        comb = get_combinations(campaign['settings'])
        combs.append({
            "name": campaign['name'],
            "combinations": comb
        })

    return combs

def generate_scripts(combination):
    script_base = ""
    
    pub_count = 0
    sub_count = 0
    
    duration_output = None
    
    for k, v in combination.items():
        if "bytes" in k:
            script_base = script_base + "-dataLen " + str(v) + " "
        elif "pub" in k:
            pub_count = v
        elif "sub" in k:
            sub_count = v
        elif "reliability" in k:
            if not v:
                script_base = script_base + "-bestEffort "
        elif "multicast" in k:
            if v:
                script_base = script_base + "-multicast "
        elif "durability" in k:
            script_base = script_base + "-durability " + str(v) + " "
        elif "latency_count" in k:
            latency_count_output = "-LatencyCount " + str(v) + " "
        elif "duration" in k:
            duration_output = "-executionTime " + str(v)
    
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
            script = script + " " + duration_output + " " + latency_count_output + " -batchSize 0 "
        
        script = script + " -transport UDPv4 "
            
        updated_scripts.append(script)
           
    return updated_scripts

def allocate_scripts_per_machine(scripts, machine_count):
    
    allocated_scripts = []

    shared_pub_scripts = []
    shared_sub_scripts = []
    
    if machine_count == 1:
        return [scripts]
    else:
        for i in range(machine_count):
            shared_pub_scripts.append([])
            shared_sub_scripts.append([])
    
    pub_scripts = [x for x in scripts if '-pub' in x]
    sub_scripts = [x for x in scripts if '-sub' in x]
    
    shared_pub_scripts = share(pub_scripts, machine_count)
    shared_sub_scripts = share(sub_scripts, machine_count)

    allocated_script = {
        "pub_scripts": shared_pub_scripts,
        "sub_scripts": shared_sub_scripts,
    }

    return shared_pub_scripts, shared_sub_scripts

def machine_thread_func(machine):
    # TODO