from functions import *

args = sys.argv[1:]

# ? Get the configuration filepath.
config_path = get_config(args)

# ? Get the configuration details.
config = read_config(config_path)

# ? Generate the scripts from the configuration.
scripts = parse_config(config)

# ? Run the tests using the scripts.
run_tests(scripts)