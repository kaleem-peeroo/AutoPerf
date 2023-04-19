import warnings
from cryptography.utils import CryptographyDeprecationWarning
with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
    import paramiko
    
import json
import os
import re
import sys

from datetime import datetime
from pprint import pprint
from rich.bar import Bar
from rich.console import Console
from rich.progress import track
from rich.table import Table
console = Console()

"""
1. SSH into machine.
2. Parse the output file to find the current campaign, campaign start and expected end of campaign.
3. Parse the progress.json file to get punctual and prolonged test stats.
4. Analyse the files to get usable test stats.
"""

args = sys.argv[1:]

if len(args) != 3:
    console.log(f"3 arguments should be given. Check the README to find out how to use this.", style="bold red")
    sys.exit()
else:
    host = args[0]
    name = args[1]
    ptstdir = args[2]

# ? Connect to the controller.
    
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

key_path = "/Users/kaleem/.ssh/id_rsa"
k = paramiko.RSAKey.from_private_key_file(key_path)
ssh.connect(host, username="acwh025", pkey = k, banner_timeout=120)

# ? Check if ptstdir is valid.
sftp = ssh.open_sftp()
try:
    remote_files = sftp.listdir(ptstdir)
except Exception as e:
    console.log(f"{e}", style="bold red")
    
# ? Find the latest output file.
stdin, stdout, stderr = ssh.exec_command(f"cd {ptstdir}; ls -t *.txt | head -1")
latest_txt_file = stdout.read().decode().strip()
    
remote_txt_file = sftp.open(os.path.join( ptstdir, latest_txt_file ))
remote_txt_file_contents = remote_txt_file.read().decode("utf-8").strip()
remote_txt_file.close()

remote_txt_file_contents = remote_txt_file_contents.split("\n")

total_combination_count_line = [line for line in remote_txt_file_contents[:10] if "combinations." in line][0].strip()

total_combination_count = total_combination_count_line.split(":")[1].strip().replace(" combinations.", "")

total_combination_count = int(total_combination_count)

# ? Get campaign start.
try:
    camp_start_date_line = [line for line in remote_txt_file_contents if '[1/' in line][0]
    timestamp = re.findall(r'\[(.*?)\]', camp_start_date_line)[0]
    camp_start = timestamp
    start_date = datetime.strptime(camp_start, "%Y-%m-%d %H:%M:%S")
    now = datetime.now()

    duration = now - start_date

    days = duration.days
    seconds = duration.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    camp_duration = f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
except:
    camp_start = f"Not found in {latest_txt_file}."
    camp_duration = f"Not able to calculate because the campaign start was not found."
    
# ? Get campaign end.
try:
    camp_end = [line for line in remote_txt_file_contents if 'Campaign Expected End' in line][0].replace("Campaign Expected End Date: ", "")
    duration = datetime.strptime(camp_end, "%Y-%m-%d %H:%M:%S") - datetime.strptime(camp_start, "%Y-%m-%d %H:%M:%S")

    days = duration.days
    seconds = duration.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    expected_camp_duration = f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"
except:
    camp_end = f"Not found in {latest_txt_file}."
    
# ? Get campaign name.
camp_name = latest_txt_file.replace("_output", "").replace(".txt", "")

camp_dirs = [_ for _ in remote_files if camp_name.lower() in _.lower() and len(_.split(".")) == 1]

if len(camp_dirs) > 1:
    camp_dir = [_ for _ in camp_dirs if _.lower() == camp_name.lower()][0]
else:
    camp_dir = camp_dirs[0]
    
camp_files = sftp.listdir(os.path.join(ptstdir, camp_dir))

test_dirs = [os.path.join(ptstdir, camp_dir, _) for _ in camp_files if '.json' not in _ and '.txt' not in _]

usable_count = 0

for i in track(range(len(test_dirs)), description="Analysing tests..."):
    test_dir = test_dirs[i]
    split_string = os.path.basename(test_dir).split("_")
    s_index = [i for i, s in enumerate(split_string) if 'S' in s][0]
    s_num = int(split_string[s_index].strip('S'))
    expected_csv_count = int(s_num) + 1
    csv_count = len([_ for _ in sftp.listdir(test_dir) if '.csv' in _])
    
    if expected_csv_count == csv_count:
        usable_count += 1

try:
    json_files = [_ for _ in camp_files if ".json" in _]
    progress_json = json_files[0]
    progress_json = os.path.join( ptstdir, camp_dir, progress_json )
except Exception as e:
    console.print(f"No progress.json file found in {ptstdir}. Here are all .json files found:\n\t{json_files}", style="bold red")
    sys.exit()

# ? Read progress.json for campaign.
progress_json = sftp.open(progress_json, 'r')
progress_contents = json.load(progress_json)
progress_json.close()

# ? Count test statuses.
status_counts = {"punctual": 0, "prolonged": 0}
for item in progress_contents:
    status = item["status"]
    status_counts[status] +=1
    
# ? Get punctual, prolonged, and total test count.
punctual_test_count = status_counts['punctual']
prolonged_test_count = status_counts['prolonged']
completed_test_count = len(progress_contents)

usable_percentage = round(usable_count / completed_test_count * 100) if completed_test_count > 0 else 0

punctual_test_percent = round(punctual_test_count / completed_test_count * 100) if completed_test_count > 0 else 0
prolonged_test_percent = round(prolonged_test_count / completed_test_count * 100) if completed_test_count > 0 else 0

completed_test_percent = round(completed_test_count / total_combination_count * 100) if completed_test_count > 0 else 0

punctual_bar = Bar(
    size=100,
    begin=0,
    end=punctual_test_percent,
    color="green"
)

prolonged_bar = Bar(
    size=100,
    begin=punctual_test_percent,
    end=100,
    color="red"
)

table = Table(title=f"{name} Monitor", show_lines=True)
table.add_column("Stat")
table.add_column("Value")
table.add_row("Current Campaign", f"{camp_name}")
table.add_row("Campaign Start", f"{camp_start}")
table.add_row("Campaign Expected End", f"{camp_end}")
table.add_row("Campaign Duration", f"{camp_duration}")
table.add_row("Campaign Expected Duration", f"{expected_camp_duration}")
table.add_row("[bold blue]Completed Tests[/bold blue]", f"[bold blue]{completed_test_count}/{total_combination_count} ({completed_test_percent}%)[/bold blue]")
table.add_row("[bold green]Punctual Tests[/bold green]", f"[bold green]{punctual_test_count}/{completed_test_count} ({punctual_test_percent}%)[/bold green]")
table.add_row("[bold red]Prolonged Tests[/bold red]", f"[bold red]{prolonged_test_count}/{completed_test_count} ({prolonged_test_percent}%)[/bold red]")
table.add_row("[bold green]Usable Tests[/bold green]", f"[bold green]{usable_count}/{completed_test_count} ({usable_percentage}%)[/bold green]")

console.print(table)