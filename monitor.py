import warnings
from cryptography.utils import CryptographyDeprecationWarning
with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
    import paramiko
    
import json
import os
import re
import subprocess
import sys

from datetime import datetime
from pprint import pprint
from rich.bar import Bar
from rich.console import Console
from rich.progress import track
from rich.table import Table
from rich.markdown import Markdown

console = Console()

ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
# ? To use the above: ansi_escape.sub("", string)

def format_duration(duration):
    days = duration // (24 * 3600)
    duration = duration % (24 * 3600)
    hours = duration // 3600
    duration %= 3600
    minutes = duration // 60
    duration %= 60
    seconds = duration

    return f"{hours} Hours, {minutes} Minutes, {seconds} Seconds"

args = sys.argv[1:]

if len(args) != 4:
    console.log(f"4 arguments should be given. Check the README to find out how to use this.", style="bold red")
    sys.exit()
else:
    host = args[0]
    name = args[1]
    ptstdir = args[2]
    key_path = args[3]

console.print(Markdown(f"# {name} Monitor"))

# ? Connect to the controller.
    
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

k = paramiko.RSAKey.from_private_key_file(key_path)
ssh.connect(host, username="acwh025", pkey = k, banner_timeout=120)

# ? Check if ptstdir is valid.
sftp = ssh.open_sftp()
try:
    remote_files = sftp.listdir(ptstdir)
except Exception as e:
    console.log(f"Exception when getting remote_files: \n\t{e}", style="bold red")

remote_jsons = [file for file in remote_files if file.endswith(".json")]

if len(remote_jsons) == 0:
    console.print(Markdown("# No tests in progress."), style="bold red")
    sys.exit()
    
# ? Get the latest json file that was last edited.
latest_mtime = None
latest_json = None
for file in remote_jsons:
    file_path = os.path.join(ptstdir, file)
    mtime = sftp.stat(file_path).st_mtime
    if latest_mtime is None or mtime > latest_mtime:
        latest_mtime = mtime
        latest_file = file_path

if latest_file is None:
    console.print(Markdown("# No tests in progress."), style="bold red")
    sys.exit()

current_campaign_name = os.path.basename(latest_file).replace(".json", "").replace("_", " ").replace("statuses", "").upper().strip()
    
# ? Get the contents of the latest json file.
with sftp.open(latest_file, "r") as f:
    latest_json = f.read().decode("utf-8")
latest_json = ", ".join(latest_json.split(",")[:-1]) + "]"
try:
    latest_json = json.loads(latest_json)
except json.decoder.JSONDecodeError as e:
    console.print(Markdown(f"# The first test of {current_campaign_name} is still running. Please wait until it finishes."), style="bold red")
    sys.exit()

"""
What stats do we want to show?
- # of tests finished

Individual tests:
- start time
- end time
- duration
- statuses
- pings
- ssh pings

"""

table = Table(title=f"Stats for {current_campaign_name} ({len(latest_json)} tests)", show_header=True)
table.add_column("#", justify="left", no_wrap=True)
table.add_column("Test", justify="left", no_wrap=True)
table.add_column("Start", justify="left", no_wrap=True)
table.add_column("End", justify="left", no_wrap=True)
table.add_column("Duration", justify="left", no_wrap=True)
table.add_column("Pings", justify="left", no_wrap=True)
table.add_column("SSH Pings", justify="left", no_wrap=True)
table.add_column("Statuses", justify="left", no_wrap=False)

for test in latest_json:
    index = latest_json.index(test) + 1
    test_name = test["permutation_name"]
    start_time = test["start_time"]
    end_time = test["end_time"]
    duration = format_duration(test["duration_s"])
    
    raw_statuses = [machine["status"] for machine in test["machine_statuses"]]
    
    statuses = [f"{machine['name']}: {machine['status']}" for machine in test["machine_statuses"]]
    pings = [f"{machine['name']}: {machine['pings']}" for machine in test["machine_statuses"]]
    ssh_pings = [f"{machine['name']}: {machine['ssh_pings']}" for machine in test["machine_statuses"]]
    
    if "unreachable" in raw_statuses:
        color = "bold red"
    elif "prolonged" in raw_statuses:
        color = "bold #FFA500"
    elif "punctual" in raw_statuses:
        color = "bold green"
    else:
        color = "bold #FFA500"
    
    statuses = ", ".join(statuses)
    pings = ", ".join(str(x) for x in pings)
    ssh_pings = ", ".join(str(x) for x in ssh_pings)
    
    table.add_row(str(index), test_name, start_time, end_time, duration, pings, ssh_pings, statuses, style=color)

console.print(table)