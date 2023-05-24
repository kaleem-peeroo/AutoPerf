from functions import *

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

pprint(config_data)