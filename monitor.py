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
from rich.markdown import Markdown

console = Console()

ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
# ? To use the above: ansi_escape.sub("", string)

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

# ? Find the latest output file i.e. the latest txt file.
stdin, stdout, stderr = ssh.exec_command(f"cd {ptstdir}; ls -t | grep -vE 'stderr|stdout' | grep -E '\\.txt$' | head -1")
latest_txt_file = stdout.read().decode().strip()
latest_txt_file_exists = len(latest_txt_file) > 0
if not latest_txt_file:
    console.print(f"No output.txt files found but here are the latest zips:", style="bold red")
    zip_files = [file for file in remote_files if '.zip' in file]
    for file in zip_files:
        console.print(f"\t{file}")
    console.print("\n")
    sys.exit()

remote_txt_file = sftp.open(os.path.join( ptstdir, latest_txt_file ))
remote_txt_file_contents = remote_txt_file.read().decode("utf-8").strip()
remote_txt_file.close()
remote_txt_file_contents = ansi_escape.sub('', remote_txt_file_contents)
remote_txt_file_contents = remote_txt_file_contents.split("\n")

# ? Check if random generation mode is enabled.
rcg_lines = [line for line in remote_txt_file_contents if 'Random Generation Mode' in line]
if len(rcg_lines) > 0:
    # ? Campaign is using random generation mode.
    
    # ? Get the campaign name.
    # ? Get the config.
    config_lines = [line for line in remote_txt_file_contents if 'Config:' in line]
    if len(config_lines) == 0:
        console.log(f"Config not found. Exiting.", style="bold red")
        sys.exit()

    config_line = config_lines[0]
    config_path = config_line.split(": ")[1]
    config_path = os.path.join(ptstdir, config_path)
    
    # ? Check the config_path is valid.
    try:
        sftp.stat(config_path)
    except FileNotFoundError:
        console.print(f"Config file doesn't exist: {config_path}", style="bold red")
        sys.exit()
    
    # ? Read the config file.
    with sftp.open(config_path, 'r') as f:
        config = json.load(f)
    
    camp_name = config['name']
    total_test_count = config['generated_test_count']
    camp_settings = config['settings']
    camp_dirname = camp_name.replace(" ", "_")
    
    # ? Check if the zip exists = campaign is finished.
    camp_zipname = camp_dirname + "_raw.zip"
    is_camp_finished = camp_zipname in sftp.listdir(ptstdir)
    
    if is_camp_finished:
        camp_dirname = camp_dirname + "_raw"
        
    progress_json = os.path.join(ptstdir, camp_dirname, "progress.json")
    try:
        sftp.stat(progress_json)
    except FileNotFoundError:
        console.print(f"Couldn't find progress.json for {camp_name}.", style="bold red")
        sys.exit()
    
    test_dirs = [os.path.join(ptstdir, camp_dirname, file) for file in sftp.listdir(os.path.join(ptstdir, camp_dirname)) if not file.endswith(".txt") and not file.endswith(".json")]

    if len(test_dirs) == 0:
        console.print(f"No tests found in {camp_dirname}.", style="bold red")
        sys.exit()
    
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
    
    # ? Read progress.json for campaign.
    progress_json = sftp.open(progress_json, 'r')
    progress_contents = json.load(progress_json)
    progress_json.close()

    # ? Count test statuses.
    status_counts = {"punctual": 0, "prolonged": 0}
    statuses = []
    for item in progress_contents:
        status = item["status"]
        status_counts[status] +=1
        statuses.append(item['status'])
        
    all_statuses = []
    for status in statuses:
        if "prolonged" in status:
            all_statuses.append("🔴")
        else:
            all_statuses.append("🟢")
                
    all_statuses_output = ""
    for i, item in enumerate(all_statuses):
        all_statuses_output += f"{item} "
        if (i + 1) % 20 == 0:
            all_statuses_output += "\n"
            
    # ? Get punctual, prolonged, and total test count.
    punctual_test_count = status_counts['punctual']
    prolonged_test_count = status_counts['prolonged']
    completed_test_count = len(progress_contents)

    usable_percentage = round(usable_count / completed_test_count * 100) if completed_test_count > 0 else 0

    punctual_test_percent = round(punctual_test_count / completed_test_count * 100) if completed_test_count > 0 else 0
    prolonged_test_percent = round(prolonged_test_count / completed_test_count * 100) if completed_test_count > 0 else 0

    completed_test_percent = round(completed_test_count / total_test_count * 100) if completed_test_count > 0 else 0

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

    table = Table(title=f"Tests Stats for {camp_name}", show_lines=True, show_header=False)
    table.add_row("[bold blue]Completed Tests[/bold blue]", f"[bold blue]{completed_test_count}/{total_test_count} ({completed_test_percent}%)[/bold blue]")
    table.add_row("[bold green]Punctual Tests[/bold green]", f"[bold green]{punctual_test_count}/{completed_test_count} ({punctual_test_percent}%)[/bold green]")
    table.add_row("[bold red]Prolonged Tests[/bold red]", f"[bold red]{prolonged_test_count}/{completed_test_count} ({prolonged_test_percent}%)[/bold red]")
    table.add_row("[bold green]Usable Tests[/bold green]", f"[bold green]{usable_count}/{completed_test_count} ({usable_percentage}%)[/bold green]")
    table.add_row("All Test Statuses (20 per row)", f"{all_statuses_output}", end_section=True)
    table.add_row("[bold underline]Settings[/bold underline]", "")
    for key, value in camp_settings.items():
        if isinstance(value, list) and len(value) == 2:
            if all(isinstance(v, int) for v in value) and value != [0, 1] and value != [True, False]:
                table.add_row(f"[bold]{key}[/bold]", f"{value[0]} ... {value[1]}")
            else:
                value_str = ", ".join(str(v) for v in value if isinstance(v, int))
                table.add_row(f"[bold]{key}[/bold]", value_str)
        else:
            table.add_row(f"[bold]{key}[/bold]", str(value[0]))    

    console.print(table)

    console.print(Markdown("# All Campaigns Finished."), style="bold green")
        
        
else:
    """
    ? All the information we need:
    - All campaigns
        - campaign name
        - status ('pending' - white, 'running' - blue, 'completed' - green)
    - Current campaign
        - start date
        - expected end date
        - current duration
        - expected duration
        - completed tests
        - punctual tests
        - prolonged tests
        - usable tests
        - all tests statuses (20 per row)    
    """
    running_camp_lines = [line for line in remote_txt_file_contents if 'Running Campaign: ' in line]

    # ? Get the config.
    ptst_start_lines = remote_txt_file_contents[:3]
    ptst_config = ptst_start_lines[1]
    ptst_config = ptst_config.split(": ")
    ptst_config_path = ptst_config[len(ptst_config) - 1]
    ptst_config_path = os.path.join(ptstdir, ptst_config_path)

    with sftp.open(ptst_config_path, 'r') as f:
        ptst_config_contents = json.load(f)

    config = ptst_config_contents['campaigns']

    camp_names = [item['name'] for item in config]

    completed_camps = []
    completed_camp_count = 0
    # ? Check if all campaigns are finished.
    for camp_name in camp_names:
        new_camp_name = camp_name.replace(" ", "_").lower() + "_raw.zip"
        if new_camp_name in [file.lower() for file in remote_files]:
            completed_camps.append(camp_name)
            completed_camp_count += 1

    # ? Get campaign statuses.
    running_camps = []
    pending_camps = []
    if len(completed_camps) != len(camp_names):
        for line in running_camp_lines:
            running_camps.append(line.split(": ")[len(line.split(": ")) - 1])
        if len(running_camps) > 1:
            completed_camps = running_camps[:-1]    
        pending_camps = list( set(camp_names) - set(running_camps) )

    camp_status_table = Table(title="All Campaigns", show_lines=True)
    camp_status_table.add_column("#")
    camp_status_table.add_column("Campaign")
    camp_status_table.add_column("Status")

    camp_count = 1

    for camp in completed_camps:
        camp_status_table.add_row(f"{camp_count}", f"[bold blue]{camp}[/bold blue]", "[bold blue]completed[/bold blue]")
        camp_count += 1

    try:
        camp_status_table.add_row(f"{camp_count}", f"[bold green]{running_camps[0]}[/bold green]", "[bold green]running[/bold green]")
        camp_count += 1
    except IndexError:
        pass

    for camp in pending_camps:
        camp_status_table.add_row(f"{camp_count}", f"[bold white]{camp}[/bold white]", "[bold white]pending[/bold white]")
        camp_count += 1
        
    console.print(camp_status_table, style="bold white")

    if len(completed_camps) == len(camp_names):
        console.print(Markdown(f"# All campaigns have ended."), style="bold green")
        sys.exit()

    console.print(Markdown("---"))

    # ? Get the current campaign.
    current_camp_name = running_camps[len(running_camps) - 1]
    camp_running_line = [line for line in running_camp_lines if current_camp_name in line][0]

    if "[1;37m" in camp_running_line:
        camp_running_line = ansi_escape.sub('', camp_running_line)
        

    date_match = re.search(r"\[(\d{4}-\d{2}-\d{2})", camp_running_line)
    time_match = re.search(r"(\d{2}:\d{2})", camp_running_line)

    if date_match and time_match:
        date_value = date_match.group(1)
        time_value = time_match.group(1)
    else:
        console.print(f"Couldn't get the date from {camp_running_line}.", style="bold red")
        sys.exit()

    start_date = f"{date_value} {time_value}"

    duration = datetime.now() - datetime.strptime(start_date, "%Y-%m-%d %H:%M")

    days = duration.days
    seconds = duration.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60

    current_duration = f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"

    expected_duration_line = [line for line in remote_txt_file_contents if current_camp_name in line and "Days" in line][0]
    expected_duration = re.search(r'\d+ Days, \d+ Hours, \d+ Minutes, \d+ Seconds', expected_duration_line).group()
    expected_end_date = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}', expected_duration_line).group()

    current_campaign_stats = {
        "name": current_camp_name,
        "start_date": start_date,
        "expected_end_date": expected_end_date,
        "current_duration": current_duration,
        "expected_duration": expected_duration
    }

    current_camp_table = Table(title="Current Campaign Stats", show_lines=True, show_header=False)
    for key, value in current_campaign_stats.items():
        stat = key.replace("_", " ").title()
        current_camp_table.add_row(stat, value)
        
    console.print(current_camp_table, style="bold white")
    console.print(Markdown("---"))

    current_camp_filename = current_camp_name.replace(" ", "_")
    current_camp_files = [file for file in remote_files if current_camp_filename == file]
    current_camp_file = current_camp_files[len(current_camp_files) - 1]
    current_campdir = os.path.join(ptstdir, current_camp_file)

    try:
        sftp.stat(current_campdir)
    except IOError:
        console.print(f"The path {current_campdir} doesn't exist.", style="bold red")
        sys.exit()
        
    campdir_files = [os.path.join(current_campdir, file) for file in sftp.listdir(current_campdir)]

    test_dirs = [file for file in campdir_files if ".json" not in file and ".txt" not in file]

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

    json_files = [_ for _ in campdir_files if ".json" in _]
    progress_json = json_files[0]

    comb_table_lines = [line for line in remote_txt_file_contents if "Combinations Count Per Campaign" in line]
    comb_table_line = comb_table_lines[len(comb_table_lines) - 1]

    camp_dur_lines = [line for line in remote_txt_file_contents if " Campaign" in line and "Expected" in line]
    camp_dur_line = camp_dur_lines[0]

    comb_table_index_start = remote_txt_file_contents.index(comb_table_line) + 1
    comb_table_index_end = remote_txt_file_contents.index(camp_dur_line) - 1

    camp_combs_table = remote_txt_file_contents[comb_table_index_start: comb_table_index_end]

    camp_comb_line = [line for line in camp_combs_table if current_camp_name in line][0]

    try:
        current_camp_combinations = int(re.findall(r'\d+', camp_comb_line)[-1])
    except Exception as e:
        console.print(f"Couldn't get the combination count from \n\t{camp_comb_line}", style="bold red")
        sys.exit()

    # ? Read progress.json for campaign.
    progress_json = sftp.open(progress_json, 'r')
    progress_contents = json.load(progress_json)
    progress_json.close()

    # ? Count test statuses.
    status_counts = {"punctual": 0, "prolonged": 0}
    statuses = []
    for item in progress_contents:
        status = item["status"]
        status_counts[status] +=1
        statuses.append(item['status'])
        
    all_statuses = []
    for status in statuses:
        if "prolonged" in status:
            all_statuses.append("🔴")
        else:
            all_statuses.append("🟢")
                
    all_statuses_output = ""
    for i, item in enumerate(all_statuses):
        all_statuses_output += f"{item} "
        if (i + 1) % 20 == 0:
            all_statuses_output += "\n"
            
    # ? Get punctual, prolonged, and total test count.
    punctual_test_count = status_counts['punctual']
    prolonged_test_count = status_counts['prolonged']
    completed_test_count = len(progress_contents)

    usable_percentage = round(usable_count / completed_test_count * 100) if completed_test_count > 0 else 0

    punctual_test_percent = round(punctual_test_count / completed_test_count * 100) if completed_test_count > 0 else 0
    prolonged_test_percent = round(prolonged_test_count / completed_test_count * 100) if completed_test_count > 0 else 0

    completed_test_percent = round(completed_test_count / current_camp_combinations * 100) if completed_test_count > 0 else 0

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

    table = Table(title=f"Tests Stats for {current_camp_name}", show_lines=True, show_header=False)
    table.add_row("[bold blue]Completed Tests[/bold blue]", f"[bold blue]{completed_test_count}/{current_camp_combinations} ({completed_test_percent}%)[/bold blue]")
    table.add_row("[bold green]Punctual Tests[/bold green]", f"[bold green]{punctual_test_count}/{completed_test_count} ({punctual_test_percent}%)[/bold green]")
    table.add_row("[bold red]Prolonged Tests[/bold red]", f"[bold red]{prolonged_test_count}/{completed_test_count} ({prolonged_test_percent}%)[/bold red]")
    table.add_row("[bold green]Usable Tests[/bold green]", f"[bold green]{usable_count}/{completed_test_count} ({usable_percentage}%)[/bold green]")
    table.add_row("All Test Statuses (20 per row)", f"{all_statuses_output}")

    console.print(table)