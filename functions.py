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
    # dir_name = create_dir("test_combinations")
    # TODO: Write combinations to file per campaign in the dir_name folder that was created in the immediate line above this.
    
    None

    # return dir_name