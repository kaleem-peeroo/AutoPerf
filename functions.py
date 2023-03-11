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

        console.print(f"{DEBUG}Written test combinations to {comb_filename}.", style="bold green") if DEBUG_MODE else None

    return dir_name

def get_combinations_from_file(dirpath, config):
    comb_files = os.listdir(dirpath)
    comb_files = [os.path.join(dirpath, file) for file in comb_files]

    combs = []
    for file in comb_files:
        camp_name = os.path.basename(file).replace(".json", "")
        
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