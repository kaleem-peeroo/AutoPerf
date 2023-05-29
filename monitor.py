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

def format_duration(duration, units):
    days = duration // (24 * 3600)
    duration = duration % (24 * 3600)
    hours = duration // 3600
    duration %= 3600
    minutes = duration // 60
    duration %= 60
    seconds = duration

    days = int(days)
    hours = int(hours)
    minutes = int(minutes)
    seconds = int(seconds)

    days_str = f"{days} Days, " if days > 0 else ""
    hours_str = f"{hours} Hrs, " if hours > 0 else ""
    minutes_str = f"{minutes} Mins, " if minutes > 0 else ""
    seconds_str = f"{seconds} Secs" if seconds > 0 else ""
    
    output_str = ""
    if "d" in units:
        output_str += days_str
    if "h" in units:
        output_str += hours_str
    if "m" in units:
        output_str += minutes_str
    if "s" in units:
        output_str += seconds_str

    return output_str

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

table = Table(title=f"Stats for {current_campaign_name} ({len(latest_json)} tests)", show_header=True)
table.add_column("#", justify="left", no_wrap=True, min_width=5)
table.add_column("Test", justify="left", no_wrap=True)
table.add_column("Start", justify="left", no_wrap=True)
table.add_column("End", justify="left", no_wrap=True)
table.add_column("Duration", justify="left", no_wrap=True, min_width=15)
table.add_column("Pings", justify="left", no_wrap=True)
table.add_column("SSH Pings", justify="left", no_wrap=True)
table.add_column("Statuses", justify="left", no_wrap=True)

# ? Limit to the last 20 tests.
table.add_row("...", "...", "...", "...", "...", "...", "...", "...", style="bold green")
for test in latest_json[-10:]:
    index = latest_json.index(test) + 1
    test_name = test["permutation_name"]
    start_time = test["start_time"]
    end_time = test["end_time"]
    duration = format_duration(test["duration_s"], "hms")

    raw_statuses = [machine["status"] for machine in test["machine_statuses"]]
    
    statuses = [f"{machine['name']}: {machine['status']}" for machine in test["machine_statuses"]]
    statuses = sorted(statuses, key=lambda x: x.split(":")[0])
    pings = [f"{machine['name']}: {machine['pings']}" for machine in test["machine_statuses"]]
    ssh_pings = [f"{machine['name']}: {machine['ssh_pings']}" for machine in test["machine_statuses"]]
    
    avg_ping = str(int(sum([machine['pings'] for machine in test["machine_statuses"]]) / len([machine['pings'] for machine in test["machine_statuses"]])))
    avg_ssh_ping = str(int(sum([machine['ssh_pings'] for machine in test["machine_statuses"]]) / len([machine['ssh_pings'] for machine in test["machine_statuses"]])))
    
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
    
    table.add_row(str(index), test_name, start_time, end_time, duration, avg_ping, avg_ssh_ping, statuses, style=color)

console.print(table)

# ? Check how long ago the last test ended.
last_end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
current_time = datetime.now()
time_elapsed = current_time - last_end_time
time_elapsed_secs = time_elapsed.total_seconds()
time_elapsed = format_duration(time_elapsed_secs, "dhms")

# ? Get the duration from the test name
match = re.search(r'(\d+)SEC', test_name)
if match:
    test_duration_secs = int(match.group(1))
else:
    test_duration_secs = 86400 / 2

# ? Make red if been longer than a day.
if time_elapsed_secs > (test_duration_secs * 2):
    color = "bold red"
else:
    color = "bold green"

console.print(Markdown(f"# Last Test Ended {time_elapsed} Ago"), style=color)